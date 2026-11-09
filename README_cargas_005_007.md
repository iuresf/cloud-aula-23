# Aula 23 — Cargas incrementais 005, 006 e 007

Este material é **continuação** dos lotes 001–004. Não execute `terraform apply` de novo para enviar mais dados.
O projeto atual espera `entrada/lote_N/...` e processa somente quando o arquivo `manifestos/lote_N/ready.json` é enviado.
O helper `preparar_manifesto.py` descobre as versões efetivas do S3 e compara SHA256; não invente version_id e não envie manifestos antes dos arquivos.

## Preparo

1. Provisione `01_warehouse` apenas uma vez, conforme README.md e REQUISITOS_IAM.md.
2. Conclua lotes `001` e `002` do projeto original e `003` e `004` do material anterior (ou deste pacote completo).
3. Use a raiz `Aula_23` como diretório de trabalho e configure as credenciais AWS.
4. Obtenha o nome do bucket com `cd 01_warehouse && terraform output -raw ingestion_bucket` e substitua `BUCKET` nos comandos (o nome é apenas placeholder).
5. Depois de cada `ready.json`, espere `public.etl_lotes` registrar o lote antes de enviar o seguinte.

## Dia 5 — lote_005: dois pedidos novos

Arquivos: `dados/dia5/pedidos.csv`, `dados/dia5/pagamentos.jsonl`

```bash
aws s3api put-object --bucket BUCKET --key entrada/lote_005/pedidos.csv --body dados/dia5/pedidos.csv
aws s3api put-object --bucket BUCKET --key entrada/lote_005/pagamentos.jsonl --body dados/dia5/pagamentos.jsonl
python 01_warehouse/preparar_manifesto.py --bucket BUCKET --lote lote_005 --arquivo pedidos=dados/dia5/pedidos.csv=entrada/lote_005/pedidos.csv --arquivo pagamentos=dados/dia5/pagamentos.jsonl=entrada/lote_005/pagamentos.jsonl --saida ready_005.json
aws s3api put-object --bucket BUCKET --key manifestos/lote_005/ready.json --body ready_005.json --content-type application/json
```

Resultado esperado: **7 pedidos**, receita reconhecida **R$ 240,30**; P1047 site R$ 85,50 e P1048 app R$ 19,90.

## Dia 6 — lote_006: pedidos novos e dimensão automática

Arquivos: `dados/dia6/pedidos.csv`, `dados/dia6/pagamentos.jsonl`

```bash
aws s3api put-object --bucket BUCKET --key entrada/lote_006/pedidos.csv --body dados/dia6/pedidos.csv
aws s3api put-object --bucket BUCKET --key entrada/lote_006/pagamentos.jsonl --body dados/dia6/pagamentos.jsonl
python 01_warehouse/preparar_manifesto.py --bucket BUCKET --lote lote_006 --arquivo pedidos=dados/dia6/pedidos.csv=entrada/lote_006/pedidos.csv --arquivo pagamentos=dados/dia6/pagamentos.jsonl=entrada/lote_006/pagamentos.jsonl --saida ready_006.json
aws s3api put-object --bucket BUCKET --key manifestos/lote_006/ready.json --body ready_006.json --content-type application/json
```

Resultado esperado: **9 pedidos**, receita **R$ 362,80**; a `dim_canal` recebe o novo membro **marketplace**. P1049 marketplace R$ 110,00; P1050 site R$ 12,50.

## Dia 7 — lote_007: estorno sem novo pedido

Este lote contém **apenas pagamento**, porque P1048 já existe na FATO_PEDIDOS. Não envie um CSV de pedidos neste lote.

```bash
aws s3api put-object --bucket BUCKET --key entrada/lote_007/pagamentos.jsonl --body dados/dia7/pagamentos.jsonl
python 01_warehouse/preparar_manifesto.py --bucket BUCKET --lote lote_007 --arquivo pagamentos=dados/dia7/pagamentos.jsonl=entrada/lote_007/pagamentos.jsonl --saida ready_007.json
aws s3api put-object --bucket BUCKET --key manifestos/lote_007/ready.json --body ready_007.json --content-type application/json
```

Resultado esperado: **9 pedidos**, receita **R$ 342,90**; a receita do P1048 passa de R$ 19,90 para **zero**. A data de `dim_tempo` para P1048 continua **24/10**, pois representa a data do pedido, não a do estorno.

## Consultas de verificação no Redshift

Use Query Editor v2 e execute `01_warehouse/sql/07_verificar_incrementais.sql` **após** o lote_007. As consultas incluem:
- `etl_lotes` confirmando lotes concluídos;
- pedidos existentes e receita atual;
- detalhamento por canal (marketplace) e por dia do pedido;
- estado atual de P1048 (estornado, receita zero).

## Falhas e repetição

- Mesmo `lote_id`/mesmo manifesto: a procedure não duplica a publicação; o job termina indicando que foi processado.
- Mesmo `lote_id` com conteúdo diferente: a carga rejeita. Para alterações legítimas, envie **outro lote**.
- Pagamento em instante mais antigo não regride o estado mais recente; em timestamp igual com resultados diferentes, a carga rejeita.
- Pagamento que referencia pedido inexistente falha; publique o pedido e reenvie o delta depois.
- Falha do Glue: verificar Step Functions, Glue JobRun, CloudWatch, `rejeitados/` e `etl_lotes`. A SQS DLQ captura falha de **entrega de evento**, não todo erro do ETL.
- O projeto não tem CDC nem agendamento diário. Enviar arquivos e manifesto é manual; para automação de fontes, seria necessária integração adicional.
- Não foram realizados testes em conta AWS. AWS Academy pode restringir Glue, Redshift Serverless, Step Functions, IAM e VPC.
