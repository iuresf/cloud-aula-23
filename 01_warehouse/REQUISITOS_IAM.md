# Requisitos: roles existentes e rede

Este Terraform **nao cria roles ou politicas IAM** e nao altera trusts de roles existentes. Os quatro ARNs podem coincidir somente se a role ja tiver os trusts e permissoes de todos os servicos envolvidos. O nome LabRole, sozinho, nao comprova isso.

Quando nenhum ARN e informado, os quatro servicos usam a LabRole da conta das
credenciais AWS ativas. Cada variavel abaixo pode substitui-la individualmente.
O processo Terraform continua usando suas proprias credenciais; nenhuma chamada
AssumeRole ou alteracao da role do CloudFormation e implicita neste padrao.

| Variavel | Principais de servico no trust existente | Permissoes necessarias (escopo nos recursos deste projeto) |
|---|---|---|
| glue_role_arn | glue.amazonaws.com | S3 ListBucket/GetObject/GetObjectVersion em fontes e assets; PutObject nas areas tecnica/evidencias/rejeitados; EC2 ENIs/Describe; CloudWatch Logs; redshift:GetClusterCredentials; Data API ExecuteStatement/BatchExecuteStatement/DescribeStatement/GetStatementResult/CancelStatement |
| redshift_s3_role_arn | redshift.amazonaws.com | S3 ListBucket, GetObject e PutObject no prefixo temporario usado pelo conector; ja associada ao cluster existente |
| sfn_role_arn | states.amazonaws.com | glue:StartJobRun/GetJobRun/GetJobRuns/BatchStopJobRun; s3:PutObject em `auditoria/tentativas/*`; permissoes de log delivery do Step Functions/CloudWatch |
| events_role_arn | events.amazonaws.com | states:StartExecution para a state machine desta pratica |

A identidade que executa Terraform precisa criar os recursos declarados, passar as quatro roles (iam:PassRole, nos servicos correspondentes), consultar o cluster existente, iniciar e consultar o JobRun de inicializacao (`glue:StartJobRun` e `glue:GetJobRun`) e usar/criar as service-linked roles exigidas pelo servico. O codigo cria uma **politica de recurso SQS** para receber falhas de entrega do EventBridge; isso nao e uma politica IAM de usuario/role.

As novas metricas/alarmes requerem autorizacao CloudWatch Logs/CloudWatch para
criar grupos, filtros e alarmes. A role Glue precisa `s3:PutObject` em
`auditoria/tentativas/*` e `rejeitados/*`, Data API e DDL/DML em
`etl_execucoes`, `stg_proveniencia` e `dw_proveniencia`. A role Step Functions
precisa `s3:PutObject` no prefixo de auditoria; sem isso, a execucao falha antes
de iniciar Glue. Acoes SNS sao opcionais e exigem ARN existente autorizado;
nao sao criadas subscriptions ou politicas IAM neste projeto.

A role Glue solicita credenciais temporarias para o cluster provisionado. Nao ha senha fixa no Terraform nem no script. O mesmo usuario de banco e usado no JDBC e na Redshift Data API. Ele precisa de CREATE/USAGE no schema public, criar/substituir a procedure, acesso as tabelas e permissao para COPY com a role associada. Neste laboratorio foi usado awsuser; em producao crie um usuario com os menores privilegios necessarios.

## Rede

- VPC com DNS support/hostnames habilitados; subnet Glue na mesma VPC do cluster, com IPs livres.
- Terraform cria SG Glue com comunicacao TCP interna entre workers e acrescenta uma regra 5439 ao SG existente do Redshift para o SG Glue.
- Para o Glue acessar S3 e APIs, a subnet privada precisa de NAT autorizado ou endpoints existentes. Redshift com enhanced VPC routing tambem precisa acessar S3 pela rota adequada.
- `create_private_endpoints=true` cria S3 Gateway (rotas de **todas** as subnets usadas) e Interface endpoints para Glue, Logs, Redshift Data e Redshift em uma AZ. Confirme disponibilidade regional, ausencia de endpoints com Private DNS conflitante e custos. Em producao avalie endpoints em varias AZs.
- `false` nao cria rotas/NAT/endpoints: o administrador deve confirmar conectividade existente. S3 Gateway nao substitui endpoints das APIs.

## Fontes oficiais

https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-redshift-home.html
https://docs.aws.amazon.com/glue/latest/dg/start-connecting.html
https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-security-other-services.html
https://docs.aws.amazon.com/redshift-serverless/latest/APIReference/API_GetCredentials.html
https://docs.aws.amazon.com/step-functions/latest/dg/connect-glue.html
https://docs.aws.amazon.com/step-functions/latest/dg/cw-logs.html
https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-events-iam-roles.html
