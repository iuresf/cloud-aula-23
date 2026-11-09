# Implantacao AWS Academy — 08/11/2026

Conta `795636653176`, regiao `us-east-1`, sessao Terraform `voclabs`.
Servicos executam com `arn:aws:iam::795636653176:role/LabRole` existente.

## Recursos implantados

| Componente | Recurso |
| --- | --- |
| S3 privado/versionado/SSE-S3 | `aula23-dw-795636653176` |
| Stack gerenciada pelo Terraform | `aula23-dw-landing` |
| Redshift provisionado privado | `aula23-dw`, um no `ra3.large` |
| Banco/schema | `aula23.dw` |
| Glue Python Shell | `aula23-dw-etl` |
| Step Functions Standard | `aula23-dw-pipeline` |
| EventBridge | `aula23-dw-manifest` |
| Glue Catalog | `aula23_landing` |

## Evidencias reais

O envio de `manifestos/lote01/ready.json` iniciou automaticamente uma execucao
Step Functions pelo EventBridge em 08/11/2026, 19:45:56 (America/Fortaleza).
O Glue executou com sucesso em 26 segundos (tempo reportado pelo servico).
O workflow completo concluiu com `SUCCEEDED` as 19:47:00.
JobRun: `jr_c5484c8a219b04b35ec0ecdb6caea556c05ad9e8f59b7f152535de0ff0c15257`.
Batch Data API: `3afe4a3c-cf8d-4be2-9ce3-8c60ace273a7`, FINISHED.

Consulta direta via Data API confirmou:

| Pedido | Data | Canal | Receita |
| --- | --- | --- | ---: |
| P101 | 2026-10-01 | NAO_INFORMADO | 120,00 |
| P102 | 2026-10-02 | NAO_INFORMADO | 200,00 |
| P108 | 2026-10-08 | NAO_INFORMADO | 100,00 |

Total: 3 registros, 3 chaves distintas, R$420,00, zero relacionamentos orfaos
em DIM_TEMPO/DIM_CANAL. Evidencias persistidas em
`s3://aula23-dw-795636653176/evidencias/lote01/`.
RAW=10, invalidos=3, duplicatas=1, STAGE=6, cancelados=1, pendentes=2.
Cobertura dos aprovados tecnicamente validos: 3/5 = 60% (nao qualidade global).

Quatro testes locais passaram: lote original, duplicata auxiliar, duplicata
conflitante e escape SQL. O plano Terraform posterior ao deploy retornou
`No changes`, sem diferencas entre infraestrutura e configuracao.

Uma repeticao iniciada antes da liberacao completa da capacidade Glue recebeu
`ConcurrentRunsExceededException`. O workflow passou a repetir exclusivamente
essa condicao com espera progressiva, sem repetir automaticamente erros de dados.

O reprocessamento `validacao-reprocessamento-lote01-v2` executou o Glue com
`SUCCEEDED` em 19 segundos. Uma nova consulta direta confirmou novamente
3 pedidos, 3 chaves, R$420,00 e zero relacionamentos orfaos: o mesmo lote nao
duplicou pedidos. JobRun:
`jr_382b281a8f4543f8e84cc87e49b84751a332b1080f1acc43b944826c8396098a`.
O workflow de reprocessamento concluiu com `SUCCEEDED` as 19:49:11.

## Metodos testados e adaptacoes

- `aws_s3_bucket`: falhou por SCP negando GetBucketObjectLockConfiguration.
  `aws_cloudformation_stack` criou e atualizou o bucket com as propriedades
  declaradas, mantendo as politicas organizacionais intactas.
- Redshift Serverless: CreateNamespace negado por falta de permissao.
  Redshift provisionado ra3.large single-node: criado e validado.
- LabRole: permitida nos servicos. AssumeRole pela sessao voclabs: negado.
- Python Shell/Data API substituem Spark/conector Redshift no projeto adaptado.
  Os arquivos presentes pertencem ao laboratorio anterior e nao informam canal;
  NAO_INFORMADO e explicito. O resultado implementa o modelo minimo pedido pelo
  slide 19, sem alegar reproduzir integralmente a arquitetura Spark ampliada.

## Repetir validacao

```bash
.venv/bin/python warehouse/verify_remote.py
python3 -m unittest discover -s warehouse -v
```

O Redshift permanece ativo e consome creditos. O README do Terraform inclui
comandos para pausar/retomar e instrucoes de novos lotes e manutencao do estado.
