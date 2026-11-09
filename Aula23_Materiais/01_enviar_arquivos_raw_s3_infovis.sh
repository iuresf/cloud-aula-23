#!/bin/bash

# Execute este script somente após 00_criar_buckets_s3_infovis.sh.
set -euo pipefail

MATRICULA="202602"
BUCKET_RAW="ufpi-cead-infovis-raw-$MATRICULA"

aws s3 cp "erp.csv" "s3://$BUCKET_RAW/erp/lote01/"
aws s3 cp "loja.jsonl" "s3://$BUCKET_RAW/loja/lote01/"
aws s3 cp "pagamentos.csv" "s3://$BUCKET_RAW/pagamentos/lote01/"
aws s3 cp "clientes.csv" "s3://$BUCKET_RAW/clientes/lote01/"
