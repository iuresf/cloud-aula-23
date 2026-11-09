"""Gera JSON local; nao envia arquivos, nao chama Glue nem executa ETL.
Apos o upload manual, consulta as versoes S3 e confere o SHA256 dos bytes enviados.
"""
import argparse
import hashlib
import json
from pathlib import Path
import boto3


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--bucket', required=True)
    ap.add_argument('--lote', required=True)
    ap.add_argument('--region', default='us-east-1')
    ap.add_argument('--arquivo', action='append', required=True, help='tipo=caminho_local=chave_s3; pode repetir')
    ap.add_argument('--saida', type=Path, default=Path('ready.json'))
    a = ap.parse_args()
    s3 = boto3.client('s3', region_name=a.region)
    files = []
    for spec in a.arquivo:
        kind, local, key = spec.split('=', 2)
        if kind not in ('pedidos', 'pagamentos') or not key.startswith(f'entrada/{a.lote}/'):
            raise ValueError('Tipo ou prefixo do arquivo invalido')
        head = s3.head_object(Bucket=a.bucket, Key=key)
        version = head.get('VersionId')
        if not version or version == 'null':
            raise ValueError('Bucket precisa estar versionado antes do upload')
        local_hash = hashlib.sha256(Path(local).read_bytes()).hexdigest()
        h = hashlib.sha256()
        obj = s3.get_object(Bucket=a.bucket, Key=key, VersionId=version)
        for chunk in obj['Body'].iter_chunks(chunk_size=1024*1024):
            h.update(chunk)
        if h.hexdigest() != local_hash:
            raise ValueError('Bytes locais diferem da versao enviada: ' + key)
        files.append(dict(kind=kind, key=key, version_id=version, sha256=local_hash))
    a.saida.write_text(json.dumps(dict(schema_version=1, lote_id=a.lote, files=files), indent=2)+'\n')
    print(f'Manifesto criado em {a.saida}. Envie-o MANUALMENTE para manifestos/{a.lote}/ready.json por ultimo.')


if __name__ == '__main__':
    main()
