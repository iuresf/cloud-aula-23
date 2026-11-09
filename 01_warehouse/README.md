# 01 — Warehouse gerenciado na AWS

**Upload manual S3 → EventBridge → Step Functions → Glue Spark → Redshift staging → SQL: dimensoes e fatos → consultas / BI.**

O aluno envia os arquivos originais e, por ultimo, um manifesto. A limpeza, integracao, carga intermediaria e publicacao no warehouse acontecem na AWS. O runner local nao executa ETL no projeto 01. A arquitetura ilustrada esta em `arquitetura.png`.

## 1. Preparar e provisionar

Leia `REQUISITOS_IAM.md` antes do plan: sao necessarias roles EXISTENTES com trusts apropriados para Glue, Redshift, Step Functions e EventBridge. O AWS Academy pode impedir esses recursos/permissoes; o pacote nao contorna restricoes nem presume que LabRole pode assumir todos os papeis.

Por padrao o Terraform seleciona `arn:aws:iam::<conta-ativa>:role/LabRole`
para Glue, Redshift COPY, Step Functions e EventBridge. Os quatro ARNs podem ser
sobrescritos individualmente quando necessario. O Terraform usa as credenciais
AWS ativas de quem o executa; selecionar uma role para os servicos nao faz o
Terraform assumi-la nem cria IAM. Ela deve existir, permitir os servicos e estar
associada ao cluster para o COPY. Nesta copia, `terraform.tfvars` local usa a
LabRole da conta 795636653176; o arquivo nao contem credenciais. O Serverless foi
negado pela organizacao, portanto esta versao reutiliza o cluster provisionado
existente `aula23-dw`, banco `aula23`. Ele pertence a outro estado Terraform:
este projeto nao cria nem destroi esse cluster. Foram testadas com um job da
LabRole as permissoes de Data API e credenciais JDBC temporarias.

```bash
cd 01_warehouse
# Configure cluster, banco, usuario, role, VPC, subnet Glue e rotas em terraform.tfvars.
terraform init
terraform fmt -check
terraform validate
terraform plan -out=aula23.tfplan
terraform apply aula23.tfplan
terraform output
```

O primeiro `terraform apply` cria o bucket versionado, assets, rede, Glue,
Step Functions, EventBridge e fila de falhas de
entrega. Durante o apply, o job `inicializar-modelo` executa os DDLs, instala a
procedure e confere as 11 tabelas, suas colunas obrigatorias e a procedure.
O apply so conclui com sucesso depois dessa verificacao. Esse job usa a mesma
role do Glue Spark, preservando o principal Redshift e evitando diferenca de
proprietario. Ele nao insere dados de negocio. Sem NAT, habilite endpoints
privados: eles tem custo por hora e trafego. O bucket e criado via CloudFormation
devido a restricao da conta na leitura de Object Lock pelo provider S3; sua
politica Retain preserva os objetos e versoes ao destruir a pilha.

O terminal que executa Terraform precisa de AWS CLI, Python 3, credenciais
ativas e permissoes `glue:StartJobRun`/`glue:GetJobRun`; a role Glue precisa
das permissoes Data API e DDL documentadas em `REQUISITOS_IAM.md`. O apply
aguarda ate 30 minutos pelo bootstrap; se expirar, confira o JobRun informado
antes de repetir o apply. Mudancas nos SQLs, script, cluster ou role
reexecutam a inicializacao idempotente no proximo apply. Evite novos uploads
durante um apply de atualizacao. `bootstrap_via_terraform=false` existe para
testes com provider simulado e desativa a garantia de inicializacao.

O segundo passo e enviar os arquivos e o manifesto: somente ele aciona o Glue
Spark e a insercao. O job de carga verifica a existencia do controle do modelo,
mas nao recria as tabelas nem reinstala a procedure. Para cada lote posterior,
envie novos arquivos e manifesto; nao e necessario outro apply.

O Glue usa dois workers G.1X, timeout 20 minutos, uma execucao concorrente e fila habilitada. O pipeline espera o job com `.sync`. O job usa Spark para transformar e o conector Redshift para carga em massa via S3 temporario/COPY. A Data API publica o esquema dimensional por uma procedure atomica. Cada lote tem uma unica publicacao logica; nao e uma transacao distribuida entre S3 e Redshift.

## 2. Enviar o primeiro lote ao S3 — unica entrada manual

Depois do apply, aguarde a ativacao da integracao S3/EventBridge. Use o valor de `ingestion_bucket`. A partir da raiz `Aula_23`, envie os dois arquivos pelo console S3 ou, manualmente, por estes comandos. Substitua BUCKET pelo output (sem os sinais de placeholder):

```bash
aws s3api put-object --bucket BUCKET --key entrada/lote_001/pedidos.csv --body dados/inicial/pedidos/lote_001.csv
aws s3api put-object --bucket BUCKET --key entrada/lote_001/pagamentos.jsonl --body dados/inicial/pagamentos/lote_001.jsonl
```

Nao envie `eventos`: este warehouse mede pedidos e receita; eventos de navegacao pertencem as outras praticas.

O manifesto fixa a versao S3 e SHA256 de cada arquivo. O auxiliar abaixo e **somente leitura de S3 + escrita de JSON local**; nao limpa, carrega ou dispara Glue. Instale `requirements.txt` na raiz se necessario. Ele confere se os bytes enviados sao iguais aos arquivos locais:

```bash
python 01_warehouse/preparar_manifesto.py --bucket BUCKET --lote lote_001 --arquivo pedidos=dados/inicial/pedidos/lote_001.csv=entrada/lote_001/pedidos.csv --arquivo pagamentos=dados/inicial/pagamentos/lote_001.jsonl=entrada/lote_001/pagamentos.jsonl --saida ready.json
aws s3api put-object --bucket BUCKET --key manifestos/lote_001/ready.json --body ready.json --content-type application/json
```

Tambem e possivel montar o JSON manualmente usando os VersionIds dos uploads e hashes SHA256. Exemplo estrutural (substitua os valores ilustrativos; nao envie literalmente):

```json
{
  "schema_version": 1,
  "lote_id": "lote_001",
  "files": [
    {"kind": "pedidos", "key": "entrada/lote_001/pedidos.csv", "version_id": "VERSAO_REAL", "sha256": "SHA256_REAL_64_HEXA"},
    {"kind": "pagamentos", "key": "entrada/lote_001/pagamentos.jsonl", "version_id": "VERSAO_REAL", "sha256": "SHA256_REAL_64_HEXA"}
  ]
}
```

O evento usado e Object Created do EventBridge, com `detail.bucket.name`, `detail.object.key` e `detail.object.version-id` do manifesto. A regra aceita apenas `manifestos/*/ready.json`. Upload de fontes, temporarios, evidencias ou rejeitados nao dispara outro processamento. As versoes fixadas sao copiadas para um prefixo temporario isolado antes da leitura Spark; sobrescrever a chave original nao troca os bytes deste lote.

## 3. Acompanhar a execucao e entender as transformacoes

1. Abra Step Functions, state machine exibida em `state_machine_arn`, aba Executions. Abra a execucao e o estado ProcessarLote.
2. Abra o JobRun do Glue. CloudWatch mostra logs/metricas; em erro, consulte `rejeitados/<attempt_id>/` no S3.
3. Ao concluir, veja `evidencias/<lote_id>/<attempt_id>.json` no S3 e `public.etl_lotes` no Redshift. A evidencia S3 pode falhar depois do commit: a tabela de controle no Redshift e a autoridade da publicacao.

Transformacoes reais: data brasileira → DATE, moeda → DECIMAL(12,2), canal/status padronizados, duplicata exata removida e conflitos rejeitados. Pagamentos sao ordenados pelo timestamp, com rejeicao de estados diferentes no mesmo instante. Pedidos sao imutaveis por ID. Pagamento precisa referenciar pedido conhecido; pedido precisa ter um estado de pagamento.

O Glue grava `stg_pedidos`/`stg_pagamentos`, isolados por tentativa. `sql/05_publicar.sql` atualiza o estado de origem, acrescenta datas/canais novos, preserva SKs existentes e usa MERGE nos fatos afetados. A alocacao de SK canal e serializada por lock; PK/FK do Redshift nao sao usadas como se fossem restricoes automaticamente verificadas. A procedure grava `etl_lotes` e limpa o staging da tentativa na mesma transacao. Uma falha de validacao reverte a publicacao; staging parcial pode permanecer para diagnostico e deve ser limpo pelo administrador apos investigacao.

O modelo final continua com **um pedido por linha**, `dim_tempo`, `dim_canal` e `fato_pedidos`. Nao ha Gold obrigatoria no S3 nem Athena neste projeto. Originais, scripts e temporarios no S3 sao apoio ao ETL do warehouse.

## 4. Consultar o warehouse

No Redshift Query Editor v2, escolha o cluster `aula23-dw` e banco `aula23` dos outputs. Conecte com identidade autorizada e grants nas tabelas public. Execute cada statement separadamente:

- `sql/04_consulta_redshift.sql`: receita por dia/canal; inicial = **129,90**, dois pedidos.
- `sql/06_auditoria.sql`: lotes publicados, chaves duplicadas, FKs sem correspondente e versoes de pagamentos.
- `sql/02_carga.sql` e `03_delta.sql`: consultas para acompanhar os dois momentos; nao fazem carga manual.

BI acessa o mesmo Redshift com conector, autenticacao e conectividade privada autorizados; um principal diferente do Glue exige grants.

## 5. Estorno e reprocessamento

Somente depois de lote_001 estar publicado:

```bash
aws s3api put-object --bucket BUCKET --key entrada/lote_002/pagamentos.jsonl --body dados/delta/pagamentos/lote_002.jsonl
python 01_warehouse/preparar_manifesto.py --bucket BUCKET --lote lote_002 --arquivo pagamentos=dados/delta/pagamentos/lote_002.jsonl=entrada/lote_002/pagamentos.jsonl --saida ready_delta.json
aws s3api put-object --bucket BUCKET --key manifestos/lote_002/ready.json --body ready_delta.json --content-type application/json
```

Resultado: P1042 estornado, receita total **0,00**, dois pedidos. Reenvie os **mesmos bytes** de `ready_delta.json` para a mesma chave: o novo evento encontra lote/hash ja concluido e nao duplica fatos. Reenvie o manifesto inicial depois: tambem nao regride o estorno.

`lote_id` e imutavel: conteudo diferente com o mesmo ID e rejeitado. Para novos dados use outro lote. Uma versao de pagamento mais antiga nao substitui o estado mais recente. Se o delta chegar antes da carga inicial, ele falha com referencia ausente; reenvie seu manifesto depois da inicial. Esta pratica nao implementa buffering de lotes fora de ordem nem CDC completo.

## 6. Diagnostico, custos e encerramento

- Sem execucao: confira regra, nome/chave/version-id do manifesto e ativacao S3/EventBridge. A fila SQS registra **falhas de entrega ao Step Functions**, nao falhas internas do ETL.
- Workflow FAILED: confira JobRun/CloudWatch e rejeitados S3. Falha de qualidade requer corrigir o dado em **novo lote**, nao trocar bytes do lote concluido.
- Timeout JDBC/S3/API: verifique DNS, rotas, endpoints/NAT, SGs, IPs livres e requisitos IAM/SQL. Consulte REQUISITOS_IAM.md.
- Custos: Glue, capacidade Redshift, S3, Step Functions, EventBridge, logs/SQS e Interface endpoints se habilitados. Dados pequenos tornam os custos de inicializacao relevantes; nao ha promessa de gratuidade/elegibilidade Academy.

Espere todas as execucoes terminarem; revise e destrua somente o laboratorio isolado:

```bash
cd 01_warehouse
terraform plan -destroy
terraform destroy
```

O bucket usa `force_destroy=false`: um destroy falhara enquanto houver objetos
ou versoes, protegendo os originais. Nao apague dados sem decisao explicita.
Ao migrar do projeto anterior, preserve tfstate, revise mudancas de SG/roles e
escolha cuidadosamente os lotes iniciais. Nenhum apply/destroy e executado pelo material.

## Auditoria, qualidade e proveniencia

O `etl_lotes` permanece a autoridade de publicacao concluida. A nova
`etl_execucoes` registra tentativa Glue, tentativa do workflow, ARN da
Step Functions, JobRun ID quando fornecido, versao/hash/chave do manifesto,
horarios, status, contagens, codigo de erro e prefixo de evidencia. Gravacoes
da auditoria no Redshift usam transacoes separadas da procedure. O workflow
grava `auditoria/tentativas/<workflow_attempt_id>/workflow_inicio.json` antes
de iniciar Glue e `workflow_falha.json` em caso de erro; o job escreve eventos
proprios em `auditoria/tentativas/<attempt_id>/`. O ARN da execucao e
`workflow_attempt_id` relacionam os dois. A conclusao do workflow inclui o
JobRun ID retornado pelo Glue. Se Glue nem iniciar, nao existira JobRun ID nem
linha Redshift. Caso S3, CloudWatch ou a propria Step Functions estejam
indisponiveis ao mesmo tempo, nao se pode garantir gravacao externa; consulte
o historico da execucao e alarmes de infraestrutura.

`PUBLICADO_EVIDENCIA_PENDENTE` indica commit confirmado com falha posterior;
`COMMIT_DESCONHECIDO` exige consultar `etl_lotes` antes de repetir. Reenviar
o mesmo manifesto apos commit confirmado nao duplica a publicacao. Um
manifesto com mesmo lote e hash diferente e recusado; lote com falha anterior
ao commit pode ser repetido com o mesmo manifesto. Novos dados corrigidos
exigem um novo lote, sem sobrescrever os originais.

As contagens recebidos/aceitos/rejeitados sao de validacao por linha, antes de
deduplicar registros exatos; valores nulos indicam contagem ainda indisponivel.
Rejeicoes individuais incluem lote, tentativa, tipo de origem, chave/versao e
codigo sem guardar os valores dos registros. Falhas de parser de arquivo ou
falhas referenciais no Redshift podem nao permitir contagens por linha. Erros de
SQL sao registrados como codigo e mensagem generica; o staging pode permanecer
para diagnostico. A fonte original versionada nao e modificada.
Os logs de runtime do Spark/Glue podem conter detalhes adicionais de arquivos
malformados: restrinja acesso e retencao dos grupos de log e do S3; nao use os
dados da aula para armazenar informacoes pessoais reais.

`quality_allow_partial=false` (padrao): qualquer linha invalida bloqueia a
publicacao. `true` permite rejeitar linhas individualmente, mantendo ao menos
um registro valido por tipo presente. Duplicatas identicas sao deduplicadas;
conflitos de pedidos/pagamento, arquivos corrompidos, referencias ausentes,
mesmo lote com outro hash e integridade dimensional sempre bloqueiam tudo. Um
pagamento antigo e registrado como recebido na proveniencia, sem substituir o
estado mais novo. Isso nao e um historico completo de estados de pagamentos.
O modo parcial nao substitui aprovacao das regras de negocio.

`stg_proveniencia` isola origem por tentativa; `dw_proveniencia` e preenchida
na mesma transacao que a publicacao, sem adicionar linhas a `fato_pedidos`.
Para investigar um pedido execute a consulta em `sql/06_auditoria.sql` usando
seu `pedido_id`. Ela retorna lote, tentativa, tipo, chave e versao S3. Um
registro de pagamento significa recebido, nao necessariamente aplicado.
Nao ha preenchimento retroativo automatico: lotes ja publicados permanecem em
`etl_lotes`, mas so novas publicacoes apos esta revisao geram proveniencia por
registro. Para reconstruir origens antigas e necessario conferir manifestos e
versoes originais; nao inferir ou inventar o arquivo de um pedido.

`log_retention_days` configura retencao dos grupos proprios Glue/Step Functions.
O Glue emite JSON com lote, tentativa, etapa, horario, duracao, status e codigo.
Os alarmes cobrem falhas/timeouts Step Functions e eventos de rejeicao medidos
por filtro no log. `alarm_action_arns` vazio nao envia notificacoes; informe
somente um destino ja existente e autorizado. Na conta verificada ha um topico
SNS chamado `RedshiftSNS`, mas nao foi comprovado que seu uso para esta aula
esteja autorizado: nao foi conectado. Alternativas sao CloudWatch console,
historico Step Functions e fila de falhas de entrega EventBridge.

Glue Data Quality/DQDL e uma evolucao opcional. A listagem de rulesets funcionou
na conta, mas a avaliacao em Glue Spark desta arquitetura nao foi executada;
assim nao se declara disponibilidade operacional ou regras DQDL ativas. As
validacoes sao implementadas em PySpark e SQL. Consulte a documentacao AWS para
custos, permissoes e resultados de avaliacao antes de ativar Glue Data Quality.

## DataZone / OpenLineage (opcional, nao implantado)

Amazon DataZone pode catalogar e exibir lineage OpenLineage. Em 08/11/2026,
`datazone:ListDomains` retornou `AccessDenied` na conta Academy; nao ha
permissao comprovada para criar dominio, usar `PostLineageEvent` ou avaliar
custos/orcamento. Nenhum recurso DataZone foi criado. O arquivo
`exemplos/openlineage-complete.json` e um evento ilustrativo, nao e publicado
automaticamente. Ele usa nomes de exemplo; nao enviar sem configurar dominio,
recursos, IAM, produtor e identificadores reais. Uma integracao futura deve
gerar eventos START/COMPLETE/FAIL com runId estavel por tentativa, enviar via
PostLineageEvent e depois consultar o grafo no catalogo do dominio. Essa
publicacao precisa ser instrumentada e testada: a tabela de proveniencia
local nao e captura OpenLineage automatica.

DataZone cobra por uso de requisicoes, armazenamento de metadados, computacao
e recomendacoes de IA, com franquias e custos possiveis dos servicos associados.
Nao foi possivel medir o orcamento restante desta conta; confira a tabela de
precos e consumo no portal antes de propor a evolucao empresarial.

Referencias: [lineage do DataZone](https://docs.aws.amazon.com/datazone/latest/userguide/datazone-data-lineage.html),
[API PostLineageEvent](https://docs.aws.amazon.com/datazone/latest/APIReference/API_PostLineageEvent.html),
[precos do DataZone](https://aws.amazon.com/datazone/pricing/) e
[Glue Data Quality](https://docs.aws.amazon.com/glue/latest/dg/glue-data-quality.html).

## Implantacao segura desta revisao

Atualize a implantacao original **somente com seu state Terraform original**,
variaveis e roles corretas. Nao aponte este Terraform para o state do projeto
`aula23-dw` em `infovis`: ele e outro ambiente, com outro job e banco. Na conta
examinada o Serverless retornou `AccessDenied` em `CreateNamespace`, por isso
reutilizamos o cluster provisionado existente. `aws_s3_bucket` tambem recebeu
`AccessDenied` para leitura Object Lock nesta Academy. Nao execute `apply`
sem identificar state e conferir o plano: poderia criar infraestrutura nova.
O resultado efetivo da implantacao e dos testes deve ser conferido no state,
nos JobRuns e nas tabelas; nao se deve inferir sucesso apenas a partir do plano.

## Arquivos e limites de validacao

- `main.tf`, `variables.tf`, `outputs.tf`: infraestrutura; `jobs/`: codigo executado no Glue.
- `sql/01_modelo.sql` e `05_publicar.sql`: estrutura e publicacao do modelo; o job de inicializacao as instala durante o primeiro apply.
- O job e uma implementacao **scriptada PySpark**, visivel no Glue Studio Script editor. O Terraform nao cria um diagrama de Glue Studio Visual nem um crawler; os contratos dos arquivos sao conhecidos e explicitos.
- A validacao historica esta descrita em VALIDACAO_EVOLUCAO.md; confirme sempre os resultados mais recentes na conta.

Fontes: AWS Glue Redshift connections; Step Functions Glue integration; S3 EventBridge event structure; Redshift MERGE e stored procedure transaction management; Terraform AWS Provider. Links completos em REQUISITOS_IAM.md e no material da aula.


## 7. Cargas posteriores para demonstrar novas inclusoes (005 a 007)

O pipeline aceita novos pedidos e novos canais sem novo `terraform apply`. Consulte `../README_cargas_005_007.md`, que inclui arquivos CSV/JSONL, manifestos, comandos e resultados esperados. Rode `sql/07_verificar_incrementais.sql` após concluir `lote_007`.
Atenção: `lote_007` é delta SOMENTE de pagamento. A quantidade de pedidos permanece 9, embora a receita diminua.
