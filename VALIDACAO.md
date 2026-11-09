# Validação — Aula 23 / Data Warehouse

## Organização

- Este projeto é independente das aulas 25 e 26.
- A estrutura contém infraestrutura Terraform, jobs do Glue, SQL Redshift, dados iniciais e lotes adicionais 002–007, testes e README próprios.
- Os arquivos Python foram compilados por verificação de sintaxe na preparação da pasta independente, e o ZIP passou na verificação de integridade.

## Limites

- Separar os arquivos no Google Drive não executa Terraform, Glue, Step Functions ou Redshift.
- A implementação depende de roles IAM existentes, rede, VPC, serviços habilitados e custos reais da conta (que podem ser restritos no AWS Academy).
- Os resultados em `resultados_esperados.json` são previsões do cenário didático, não resultados verificados em uma execução na AWS.
- Testes locais presentes em `01_warehouse/tests/` podem ser repetidos com `python -m pip install -r 01_warehouse/requirements-test.txt` e `PYTHONPATH=01_warehouse/jobs python -m unittest discover -s 01_warehouse/tests -v` na raiz desta aula.
- Revise e execute `terraform init`, `terraform validate` e `terraform plan` no ambiente real antes de `apply`.

## Publicação incremental

Siga `01_warehouse/README.md`, `README_cargas_003_004.md` e `README_cargas_005_007.md`. Cada novo lote exige novos arquivos S3 e publicação de um manifesto `ready.json` por último. A procedure Redshift atualiza o estado dos pedidos afetados, e os lotes não são substituição integral da tabela fato.
