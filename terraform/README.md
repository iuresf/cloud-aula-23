# Aula 23 na AWS Academy

Regiao: `us-east-1`. Requer Terraform >= 1.11 e credenciais temporarias ativas.

## Organização dos arquivos

```text
terraform/
├── main.tf                 # composição, provedor, identidade e rede
├── variables.tf            # entradas do ambiente
├── outputs.tf              # referências devolvidas após o apply
├── versions.tf             # versões dos providers
└── modules/
    ├── landing/            # bucket S3 versionado e catálogo Glue
    ├── warehouse/          # subnet group e Redshift
    ├── glue_etl/           # scripts versionados e Glue Job
    └── orchestration/      # Step Functions e EventBridge
```

Os módulos não contêm regra de negócio: `../warehouse/pipeline.py` contém a
transformação, enquanto `../warehouse/bootstrap.sql` é aplicado pelo Terraform
via Redshift Data API para preparar o banco e
`../Aula23_Materiais/` contém as fontes sintéticas. Veja também o
`../docs/CONCEITOS_WAREHOUSE.md`.

## Arquitetura

`upload_lote.py -> S3/ready.json -> EventBridge -> Step Functions -> Glue -> Redshift`

- S3 privado/versionado/SSE-S3: `entrada/`, `manifestos/`, `rejeitados/`, `evidencias/`.
- Redshift privado, um no `ra3.large`, banco `aula23`, schema `dw`.
- `DIM_TEMPO`, `DIM_CANAL`, `FATO_PEDIDOS`: uma linha publicada por pedido.
- Glue Python Shell 3.9, 0.0625 DPU, timeout 15 minutos, retries zero e concorrencia 1.
- Data API publica com batch transacional e LOCK/upsert.
- EventBridge/Step Functions Standard executam o job para novos manifestos.
- Step Functions repete somente erros de concorrencia Glue, com espera progressiva.
- Catalogo Glue `aula23_landing` disponivel; nao substitui o Redshift.

Usa `LabRole` existente para Glue/Step Functions/EventBridge. Terraform usa a
sessao `voclabs`. Nao cria roles ou chaves. A senha Redshift e efemera/write-only,
sem persistir no estado. Consultas usam credenciais temporarias da Data API.
O Glue somente transforma e carrega lotes; ele nao cria schema ou tabelas.

## Adaptacoes

`aws_s3_bucket` falhou na leitura de Object Lock negada por SCP. O bucket agora
e gerenciado por `aws_cloudformation_stack`, sem alterar a SCP. Redshift
Serverless foi negado (`CreateNamespace`); Redshift provisionado foi aceito.

Esta trilha implementa o modelo minimo do slide 19 com os arquivos disponiveis.
Usa Python Shell/Data API em lugar do Spark/conector JDBC/COPY da arquitetura
ampliada. Nao implementa snapshot periodico, Data Mart financeiro ou CDC.
Os arquivos nao informam canal: publica `NAO_INFORMADO`.

## Deploy

Na pasta `terraform`:

```bash
terraform init
terraform fmt -recursive
terraform validate
terraform plan -out=tfplan
terraform apply tfplan
```

Na raiz, instalar boto3 e enviar um lote novo:

```bash
python3 -m venv .venv
.venv/bin/pip install -r warehouse/requirements.txt
AWS_DEFAULT_REGION=us-east-1 .venv/bin/python warehouse/upload_lote.py \
  --bucket aula23-dw-795636653176 --lote lote01
```

O manifesto e enviado por ultimo; o uploader recusa sobrescrever um manifesto
existente. Use outro lote nos novos envios. Fontes sao fixadas por VersionId e
conferidas por SHA256. O evento S3 pode demorar a chegar ao EventBridge.
Diagnostique falhas no Step Functions/Glue; repita a execucao pelo Step Functions
com o mesmo evento, sem sobrescrever o manifesto. O upsert evita duplicar fatos.

O workflow habilita `PUBLICAR_PARCIAL=true` pela politica didatica do material;
execucao manual do job usa padrao `false`. Invalidos RAW e pedidos pendentes
ficam nas evidencias; fonte auxiliar duplicada bloqueia a carga.
Lote original: P101/P102/P108, R$420, cobertura 3/5 aprovados validos (60%).
Nao representa qualidade global nem receita total do negocio.
Veja `../warehouse/validation.sql` para contagens, chaves e joins.

Verificacao remota do lote original:

```bash
.venv/bin/python warehouse/verify_remote.py
```

## Custos e estado

Redshift cobra enquanto ativo. Pause apos a aula e retome antes dos novos
envios; nao ha retomada automatica:

```bash
aws redshift pause-cluster --cluster-identifier aula23-dw --region us-east-1
aws redshift resume-cluster --cluster-identifier aula23-dw --region us-east-1
```

Preserve `terraform.tfstate*`. Por padrão, `terraform destroy` exclui compute e
workflow, **não** cria snapshot final do Redshift e mantém o bucket S3 com seus
dados e versões (`retain_bucket_on_destroy = true`). Para apagar o bucket, é
necessário remover também todas as versões e delete markers; faça isso somente
depois de decidir que os dados da aula não serão mais necessários. Os dois
comportamentos são configuráveis pelas variáveis
`create_final_snapshot_on_destroy` e `retain_bucket_on_destroy`.

## Fontes

- https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/cloudformation_stack
- https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html
- https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-clusters.html
- https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_BatchExecuteStatement.html
- https://docs.aws.amazon.com/step-functions/latest/dg/connect-glue.html
