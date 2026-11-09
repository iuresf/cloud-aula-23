# Revisão técnica estática — ingestão incremental (Aula 23)

## Conclusão

Os arquivos `main.tf`, `jobs/warehouse_etl.py`, `jobs/warehouse_core.py`, `jobs/warehouse_spark.py`, `sql/01_modelo.sql` e `sql/05_publicar.sql` do projeto mais recente são compatíveis, em termos de contrato e desenho, com novos lotes `lote_005`, `lote_006` e `lote_007`.

- O Terraform configura o bucket S3 versionado, publica os scripts Glue/SQL, o job, o workflow, a regra EventBridge para `manifestos/*/ready.json` e o Redshift Serverless.
- O Terraform **não envia** novos dados em `entrada/`. Cada conjunto é enviado pelo usuário e identificado com um manifesto gerado pelo `preparar_manifesto.py`.
- `warehouse_core.parse_manifest` aceita de 1 a 20 arquivos dos tipos `pedidos` e/ou `pagamentos` associados a um `lote_id`.
- `warehouse_spark.prepare` lê CSV `;` e JSONL, valida os dados e remove duplicatas exatas.
- A procedure `public.publicar_lote` usa `INSERT` em pedidos de origem e `MERGE` em `fato_pedidos`: pedido novo é inserido; pagamento mais recente atualiza um pedido existente; também acrescenta membros de `dim_tempo` e `dim_canal` conforme necessário.
- A tabela `public.etl_lotes` evita duplicação de publicação para mesmo `lote_id`/hash. Outro conteúdo com o mesmo `lote_id` é rejeitado.
- Novos campos, novo formato de arquivo, outro tipo de entidade e CDC automático **não** são suportados pelo contrato atual sem mudanças nos scripts e no modelo.

## Dados adicionados

| Lote | Acontecimento | Pedidos acumulados | Receita atual |
| --- | --- | ---: | ---: |
| 004 | Estado inicial para o teste | 5 | R$ 134,90 |
| 005 | Inserir P1047 (+85,50) e P1048 (+19,90) | 7 | R$ 240,30 |
| 006 | Inserir P1049 (+110,00, marketplace) e P1050 (+12,50) | 9 | R$ 362,80 |
| 007 | Atualizar P1048 para estornado (−19,90 em receita atual) | 9 | R$ 342,90 |

## Verificações executadas

- `python -m compileall` dos scripts Python: concluído.
- `python -m unittest -v tests.test_contract tests.test_cargas_incrementais`: **13 testes passaram**, incluindo validação dos arquivos de 001 a 007, contratos de manifesto, novo canal e estorno.
- O pacote ZIP foi testado com `zipfile.testzip()`: íntegro.

## Limites desta verificação

**Não se executou** `terraform init`, `terraform validate`, `terraform plan`, nem `terraform apply` por indisponibilidade do executável Terraform e de conta AWS nesta sessão. Os testes `test_spark.py` requerem PySpark, indisponível no ambiente local; não foram executados aqui. Portanto o fluxo real AWS e os recursos/permissões no AWS Academy **não estão comprovados**. O trabalho consiste em revisão estática + testes locais de contrato/estado com os novos arquivos.

Depois de baixar o pacote, execute `terraform fmt -check`, `terraform validate`, `terraform plan` com variáveis e permissões reais, `pip install -r requirements-test.txt` se puder, e então os testes Spark e as cargas em um ambiente AWS autorizado. Valide `public.etl_lotes` e as consultas de `sql/07_verificar_incrementais.sql` após cada lote.
