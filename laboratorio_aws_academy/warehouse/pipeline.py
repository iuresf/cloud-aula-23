"""ETL didatico pequeno: S3 versionado -> Glue Python Shell -> Redshift Data API.

Os arquivos originais nao informam canal: usar NAO_INFORMADO explicitamente.
Nao substitui o pipeline Spark do material; e uma trilha de baixo custo para
o mesmo lote e para o modelo dimensional minimo do slide 19.
"""
import argparse
import csv
import hashlib
import io
import json
import re
import time
from datetime import datetime
from decimal import Decimal


def money(value):
    value = re.sub(r"^R\$\s*", "", value.strip())
    if not re.fullmatch(r"(?:\d+|\d{1,3}(?:\.\d{3})+),\d{2}", value):
        raise ValueError("valor_invalido")
    return Decimal(value.replace(".", "").replace(",", "."))


def transform(sources):
    def rows(name):
        return list(csv.DictReader(io.StringIO(sources[name]), delimiter=";"))

    def indexed(records, key):
        result = {}
        for row in records:
            if not row.get(key) or row[key] in result:
                raise ValueError("Fonte auxiliar com chave ausente/duplicada: " + key)
            result[row[key]] = row
        return result

    clients = indexed(rows("clientes.csv"), "cliente_id")
    shop = indexed([json.loads(line) for line in sources["loja.jsonl"].splitlines() if line.strip()], "order_id")
    payments = indexed(rows("pagamentos.csv"), "pedido_id")
    for row in payments.values():
        money(row["valor_pago"])
    for row in shop.values():
        Decimal(row["total"])
    rejected, staged, published = [], {}, []
    counts = {"raw": 0, "invalidos": 0, "duplicatas": 0, "cancelados": 0, "pendentes": 0}
    for raw in rows("erp.csv"):
        counts["raw"] += 1
        try:
            if not raw["pedido_id"].strip() or not raw["cliente_id"].strip():
                raise ValueError("chave_ausente")
            amount = money(raw["valor"])
            date = None
            for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
                try:
                    date = datetime.strptime(raw["data_venda"].strip(), fmt).date().isoformat()
                    break
                except ValueError:
                    pass
            if date is None:
                raise ValueError("data_invalida")
            status = raw["status"].strip().lower()
            if status not in ("aprovado", "cancelado"):
                raise ValueError("status_invalido")
            row = {"pedido_id": raw["pedido_id"].strip(), "cliente_id": raw["cliente_id"].strip(),
                   "data": date, "receita": str(amount), "status": status, "canal": "NAO_INFORMADO"}
        except (ValueError, KeyError) as exc:
            counts["invalidos"] += 1
            rejected.append({"origem": raw, "motivo": str(exc)})
            continue
        key = row["pedido_id"]
        if key in staged:
            if staged[key] != row:
                raise ValueError("Duplicata conflitante: " + key)
            counts["duplicatas"] += 1
            continue
        staged[key] = row
    for key, row in staged.items():
        if row["status"] == "cancelado":
            counts["cancelados"] += 1
            continue
        client, store, payment = clients.get(row["cliente_id"]), shop.get(key), payments.get(key)
        reason = None
        if not client or not store or not payment:
            reason = "referencia_ausente"
        elif store["customer_id"] != row["cliente_id"]:
            reason = "cliente_divergente"
        elif store["currency"] != "BRL" or payment["moeda"] != "BRL":
            reason = "moeda_divergente"
        elif payment["status_pagamento"].strip().lower() != "aprovado":
            reason = "pagamento_nao_aprovado"
        elif Decimal(row["receita"]) != Decimal(store["total"]) or Decimal(row["receita"]) != money(payment["valor_pago"]):
            reason = "valor_divergente"
        if reason:
            counts["pendentes"] += 1
            rejected.append({"origem": row, "motivo": reason})
        else:
            published.append(row)
    counts["stage"] = len(staged)
    counts["publicados"] = len(published)
    counts["receita"] = str(sum((Decimal(row["receita"]) for row in published), Decimal("0")))
    approved = len(staged) - counts["cancelados"]
    counts["cobertura_aprovados_validos"] = len(published) / approved if approved else 0
    return list(staged.values()), published, rejected, counts


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def load_sql(rows, lote):
    statements = ["lock table dw.fato_pedidos", "create temp table carga (pedido_id varchar(64), cliente_id varchar(64), data date, receita decimal(12,2))"]
    if rows:
        statements.append("insert into carga values " + ",".join(
            "(" + ",".join(literal(row[k]) for k in ("pedido_id", "cliente_id", "data", "receita")) + ")" for row in rows))
    statements.extend([
        "insert into dw.dim_tempo (data_calendario,ano,mes,dia,trimestre) select distinct data,extract(year from data),extract(month from data),extract(day from data),extract(quarter from data) from carga where data not in (select data_calendario from dw.dim_tempo)",
        "delete from dw.fato_pedidos using carga where dw.fato_pedidos.pedido_id=carga.pedido_id",
        "insert into dw.fato_pedidos (pedido_id,tempo_sk,canal_sk,cliente_id,receita,lote_id) select c.pedido_id,t.tempo_sk,k.canal_sk,c.cliente_id,c.receita," + literal(lote) + " from carga c join dw.dim_tempo t on c.data=t.data_calendario cross join dw.dim_canal k where k.canal_cd='NAO_INFORMADO'",
    ])
    return statements


def wait_statement(api, statement_id):
    for _ in range(180):
        result = api.describe_statement(Id=statement_id)
        if result["Status"] == "FINISHED":
            return result
        if result["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(result.get("Error", result["Status"]))
        time.sleep(2)
    api.cancel_statement(Id=statement_id)
    raise TimeoutError(statement_id)


def main():
    import boto3
    parser = argparse.ArgumentParser()
    parser.add_argument("--BUCKET", required=True)
    parser.add_argument("--MANIFEST", required=True)
    parser.add_argument("--CLUSTER", required=True)
    parser.add_argument("--PUBLICAR_PARCIAL", default="false", choices=("true", "false"))
    args, _ = parser.parse_known_args()
    s3, api = boto3.client("s3"), boto3.client("redshift-data")
    manifest = json.loads(s3.get_object(Bucket=args.BUCKET, Key=args.MANIFEST)["Body"].read())
    lote = manifest["lote"]
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", lote):
        raise ValueError("Lote invalido")
    sources = {}
    for name, obj in manifest["files"].items():
        raw = s3.get_object(Bucket=args.BUCKET, Key=obj["key"], VersionId=obj["version_id"])["Body"].read()
        if hashlib.sha256(raw).hexdigest() != obj["sha256"]:
            raise ValueError("Hash divergente: " + name)
        sources[name] = raw.decode("utf-8-sig")
    stage, published, rejected, counts = transform(sources)
    def put(key, value):
        s3.put_object(Bucket=args.BUCKET, Key=key, Body=json.dumps(value, ensure_ascii=False).encode(), ContentType="application/json")
    put("stage/" + lote + "/pedidos.json", stage)
    put("rejeitados/" + lote + "/registros.json", rejected)
    put("evidencias/" + lote + "/qualidade.json", counts)
    if counts["pendentes"] and args.PUBLICAR_PARCIAL != "true":
        raise ValueError("Publicacao parcial exige politica didatica explicita")
    # O bootstrap e as cargas sao lotes SQL separados. Cada batch e atomico.
    sql = s3.get_object(Bucket=args.BUCKET, Key="scripts/bootstrap.sql")["Body"].read().decode()
    sql = re.sub(r"--[^\n]*", "", sql)
    bootstrap = [stmt.strip() for stmt in sql.split(";") if stmt.strip() and stmt.strip().lower() not in ("begin", "commit")]
    connection = {"ClusterIdentifier": args.CLUSTER, "Database": "aula23", "DbUser": "awsuser"}
    wait_statement(api, api.batch_execute_statement(Sqls=bootstrap, **connection)["Id"])
    statement = api.batch_execute_statement(Sqls=load_sql(published, lote), **connection)["Id"]
    wait_statement(api, statement)
    put("evidencias/" + lote + "/publicacao.json", {"statement_id": statement, "status": "FINISHED", **counts})
    print(json.dumps(counts))


if __name__ == "__main__":
    main()
