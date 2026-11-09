"""Envia fontes versionadas e publica manifesto por ultimo. Nao dispara Glue."""
import argparse
import hashlib
import json
import re
from pathlib import Path


def main():
    import boto3
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--lote", default="lote01")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.lote):
        raise ValueError("Lote invalido")
    s3 = boto3.client("s3")
    manifest_key = "manifestos/" + args.lote + "/ready.json"
    # Nao modificar um lote cujo manifesto ja existe.
    try:
        s3.head_object(Bucket=args.bucket, Key=manifest_key)
    except s3.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise
    else:
        raise ValueError("Manifesto ja existe: escolha um novo lote")
    files = {}
    base = Path(__file__).resolve().parents[1] / "Aula23_Materiais"
    for name in ("erp.csv", "clientes.csv", "loja.jsonl", "pagamentos.csv"):
        raw = (base / name).read_bytes()
        key = "entrada/" + args.lote + "/" + name
        result = s3.put_object(Bucket=args.bucket, Key=key, Body=raw)
        files[name] = {"key": key, "version_id": result["VersionId"], "sha256": hashlib.sha256(raw).hexdigest()}
    s3.put_object(Bucket=args.bucket, Key=manifest_key, Body=json.dumps({"lote": args.lote, "files": files}).encode(), ContentType="application/json", IfNoneMatch="*")
    print(manifest_key)


if __name__ == "__main__":
    main()
