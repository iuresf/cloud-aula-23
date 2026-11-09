# Aula 23 — exemplos adicionais de cargas diárias

Estes arquivos são complementares ao pacote 01_warehouse já existente.
Não execute Terraform novamente para cada lote.

1. Conclua lote_001 e lote_002 do README original.
2. Salve os arquivos fornecidos sob dados/dia3 e dados/dia4 na raiz de Aula_23.
3. Envie primeiro os arquivos de lote_003, depois gere e envie o manifesto, conforme os comandos abaixo.
4. Espere a publicação no Redshift antes de iniciar o lote_004.

Dia 3:

    aws s3api put-object --bucket BUCKET --key entrada/lote_003/pedidos.csv --body dados/dia3/pedidos.csv
    aws s3api put-object --bucket BUCKET --key entrada/lote_003/pagamentos.jsonl --body dados/dia3/pagamentos.jsonl
    python 01_warehouse/preparar_manifesto.py --bucket BUCKET --lote lote_003 --arquivo pedidos=dados/dia3/pedidos.csv=entrada/lote_003/pedidos.csv --arquivo pagamentos=dados/dia3/pagamentos.jsonl=entrada/lote_003/pagamentos.jsonl --saida ready_003.json
    aws s3api put-object --bucket BUCKET --key manifestos/lote_003/ready.json --body ready_003.json --content-type application/json

Dia 4:

    aws s3api put-object --bucket BUCKET --key entrada/lote_004/pedidos.csv --body dados/dia4/pedidos.csv
    aws s3api put-object --bucket BUCKET --key entrada/lote_004/pagamentos.jsonl --body dados/dia4/pagamentos.jsonl
    python 01_warehouse/preparar_manifesto.py --bucket BUCKET --lote lote_004 --arquivo pedidos=dados/dia4/pedidos.csv=entrada/lote_004/pedidos.csv --arquivo pagamentos=dados/dia4/pagamentos.jsonl=entrada/lote_004/pagamentos.jsonl --saida ready_004.json
    aws s3api put-object --bucket BUCKET --key manifestos/lote_004/ready.json --body ready_004.json --content-type application/json

Resultado esperado no Warehouse atual após lote_004:
- 5 pedidos na fato;
- P1042 estornado, receita 0,00;
- P1044 app 45,00; P1045 site 30,00; P1046 site 59,90;
- Receita atual acumulada: 134,90.

Importante: pacote não executado em conta AWS. Necessários roles e serviços AWS permitidos, conforme README e REQUISITOS_IAM.md.
