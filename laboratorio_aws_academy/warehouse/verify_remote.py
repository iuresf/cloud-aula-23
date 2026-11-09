"""Valida o lote original no Redshift, sem alterar dados."""
import argparse
import json
from decimal import Decimal
import boto3
from pipeline import wait_statement


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", default="aula23-dw")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()
    api = boto3.client("redshift-data", region_name=args.region)
    def query(sql):
        statement = api.execute_statement(ClusterIdentifier=args.cluster, Database="aula23", DbUser="awsuser", Sql=sql)["Id"]
        wait_statement(api, statement)
        result = api.get_statement_result(Id=statement)
        return [[next(iter(field.values())) for field in row] for row in result["Records"]]
    totals = query("select count(*),count(distinct pedido_id),sum(receita) from dw.fato_pedidos")
    assert totals[0][:2] == [3, 3], totals
    assert Decimal(totals[0][2]) == Decimal("420.00"), totals
    assert query("select count(*) from dw.fato_pedidos f left join dw.dim_tempo t on t.tempo_sk=f.tempo_sk left join dw.dim_canal c on c.canal_sk=f.canal_sk where t.tempo_sk is null or c.canal_sk is null") == [[0]]
    detail = query("select f.pedido_id,t.data_calendario,c.canal_cd,f.receita from dw.fato_pedidos f join dw.dim_tempo t on t.tempo_sk=f.tempo_sk join dw.dim_canal c on c.canal_sk=f.canal_sk order by 1")
    print(json.dumps({"totals": totals, "orders": detail, "validation": "PASSED"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
