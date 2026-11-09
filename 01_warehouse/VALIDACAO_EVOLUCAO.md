# Validacao da evolucao — 08/11/2026

Projeto: `Documentos/ead/nuvem/aula23/01_warehouse`. Os arquivos e lotes 001–007
foram preservados; o projeto de `infovis/.../terraform` e outro ambiente.

## Testes realizados

- Terraform `fmt -check` e `validate`: aprovados com provider AWS 6.72.0.
- `terraform test`: dois testes com provider simulado aprovados; nenhum recurso
  AWS e criado por ele. A definicao Step Functions gerada com dados simulados
  tambem foi submetida a `ValidateStateMachineDefinition`: `OK`, sem avisos.
- 11 testes de contrato e 4 testes dos dados incrementais: aprovados.
- 5 testes PySpark 3.5.4, Python 3.11 e Java 17: aprovados, incluindo rejeicao
  parcial, mascaramento e mapeamento de arquivo/versao. O Python 3.14 do host
  nao foi compativel com o serializador do Spark 3.5; foi usado ambiente isolado.
- 2 testes com injecao de falhas e APIs simuladas: aprovados para falha antes
  do Redshift e falha da evidencia S3 apos commit confirmado. Sao testes do
  driver, nao uma falha real de S3 na AWS.
- Procedure original evoluida executada no Redshift provisionado ja existente
  `aula23-dw`/banco `aula23`, schema isolado `a23_test_c9bae877fac0`, com
  dados sinteticos: repeticao de lote, mesmo ID/hash diferente, pedido
  conflitante, pagamento sem pedido, rollback, reprocessamento de lote rejeitado,
  versao antiga de pagamento, evidencia por arquivo e repeticao apos commit.
  O teste passou e confirmou dois pedidos, receita 20,00 e estado mais recente.
  Timestamp de auditoria ISO foi inserido com sucesso no mesmo schema.
- O primeiro teste real revelou um defeito anterior: Redshift rejeitou alias na
  tabela destino do MERGE. Foi corrigido e a suite passou na segunda tentativa.
  O schema sintetico daquela primeira tentativa, `a23_test_be51a06e18fe`, e o
  schema do teste bem-sucedido foram mantidos; nenhum dado anterior foi apagado.

## Limites e verificacoes AWS

Nao foi feito apply/deploy deste projeto: nao foi encontrado state Terraform
original, nao ha workgroups/namespaces Redshift Serverless na conta e o teste
anterior de CreateNamespace retornou AccessDenied. O Glue existente
`aula23-dw-etl` e Python Shell do outro projeto, nao o job Spark deste projeto.
As mudancas portanto nao foram executadas ponta a ponta no Glue/SFN Serverless.
Bucket `aula23-dw-795636653176` e cluster `aula23-dw` continuam pertencendo ao
outro projeto; somente os schemas sinteticos de teste foram acrescentados ao
cluster existente, sem substituir os recursos/dados daquele ambiente.

`datazone:ListDomains` retornou AccessDenied. A consulta
`glue:ListDataQualityRulesets` foi permitida, sem rulesets; isso nao prova
permissao de criar/executar avaliacoes DQDL nem que o ambiente Spark exista.
Ha topico SNS `RedshiftSNS`, mas sua destinacao/autorizacao nao foi confirmada;
os alarmes ficam sem acoes por padrao. Nada foi publicado ao DataZone ou SNS.

O Redshift provisionado ja estava ativo e continua ativo, sujeito a custos.

## Duas fases — teste de 08/11/2026

O primeiro `terraform apply` foi ajustado para iniciar o job
`inicializar-modelo`, que cria e verifica as tabelas/colunas e procedure antes
de habilitar a carga. O segundo passo envia arquivos e manifesto; o job Spark
so valida e carrega, sem criar tabelas. Cinco testes de bootstrap simulado
passaram, inclusive ausencia de coluna/procedure e ausencia de carga de negocio.

O mesmo codigo de bootstrap foi exercitado contra o Redshift provisionado ja
existente, com adaptador de teste para um schema sintetico isolado
`a23_bootstrap_f455b06722`: as 11 tabelas e a procedure foram verificadas;
o fato tinha zero linhas. Depois uma insercao sintetica via staging e procedure
produziu 1 pedido e receita 10,00. O schema de teste permanece para auditoria.
Nao e um deploy real da arquitetura Glue Spark/Serverless.

O `terraform destroy` no diretorio do projeto retornou `0 destroyed`, pois nao
existe state desse projeto na conta/workspace. Nenhum recurso do projeto
anterior `aula23-dw` foi destruido. O bootstrap no Glue de producao ainda nao
foi executado: a criacao Serverless da conta deve ser autorizada primeiro.
Uma tentativa direcionada de `terraform apply` para o namespace `a23-dw`
retornou `AccessDeniedException` em `redshift-serverless:CreateNamespace`;
nenhum namespace foi criado. Nao se pode concluir o primeiro apply completo,
nem carregar o projeto Spark nessa conta enquanto persistir a restricao.

Um segundo teste passou a role existente `LabRole` ao CloudFormation por
`iam:PassRole`, sem alterar permissoes. A stack de teste tentou criar um
namespace com o principal `assumed-role/LabRole/AWSCloudFormation` e recebeu
tambem `AccessDeniedException` para `redshift-serverless:CreateNamespace`,
por ausencia de allow na identidade. O rollback removeu o namespace de teste.
A `LabRole` confia em `redshift.amazonaws.com`, mas nao em
`redshift-serverless.amazonaws.com`, principal que a documentacao AWS exige
para acesso do Serverless a outros servicos. Nenhuma permissao foi alterada.
As quatro variaveis de role estao preenchidas localmente em `terraform.tfvars`
com a `LabRole`; VPC/subnets continuam a configurar e validar separadamente.

## Reproduzir

```bash
terraform fmt -check -recursive
terraform validate
terraform test
python -m unittest discover -s tests -p 'test_contract.py' -v
python -m unittest discover -s tests -p 'test_cargas_incrementais.py' -v
python -m unittest discover -s tests -p 'test_attempts.py' -v
python -m unittest discover -s tests -p 'test_spark.py' -v
```

`test_spark.py` requer PySpark 3.5.4 e Java 17; Python 3.11 foi utilizado aqui.
O teste real Redshift e opt-in (`AULA23_TEST_REDSHIFT=1`) e cria um schema novo
de teste com dados sinteticos que permanece apos a execucao. Configure tambem
`AULA23_TEST_CLUSTER`, `AULA23_TEST_DATABASE`, `AULA23_TEST_USER` e regiao para
um ambiente explicitamente apropriado. Ele nao testa Glue Spark real ou IAM da
role do job. Nao aponte para bases de producao sem revisar o teste.
