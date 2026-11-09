# Aula 23 — Data Warehouse na AWS (projeto independente)

**Escopo:** S3 → EventBridge → Step Functions → AWS Glue Spark → Redshift Serverless.
Esta pasta é autônoma; não depende das aulas 25 (Data Lake) e 26 (Lakehouse).

## 1. Preparar

- Python >=3.10, Terraform >=1.5, AWS CLI e credenciais temporárias autorizadas.
- Leia `01_warehouse/REQUISITOS_IAM.md`. Recursos dependem das permissões da conta; AWS Academy pode restringi-los.
- Instale `python -m pip install -r requirements.txt` e execute `aws sts get-caller-identity`.
- Os arquivos de laboratório estão em `dados/`; o próprio Terraform não faz upload desses dados.

## 2. Provisionar

```bash
cd 01_warehouse
cp terraform.tfvars.example terraform.tfvars
# Ajuste VPC, subnets e roles existentes autorizadas.
terraform init
terraform validate
terraform plan -out=aula23.tfplan
terraform apply aula23.tfplan
terraform output
cd ..
```

## 3. Publicar lotes

Leia `01_warehouse/README.md` e `README_cargas_005_007.md`.
Envie `dados/inicial/pedidos/lote_001.csv` e `dados/inicial/pagamentos/lote_001.jsonl`, gere `ready.json` com `01_warehouse/preparar_manifesto.py`, e publique-o por último no S3.
Após processamento, suba os lotes 002 a 007 em ordem. O `ready.json` dispara a Step Functions e o Glue; novos lotes não exigem novo `terraform apply`.
O CSV de eventos de navegação foi retirado desta aula porque não pertence ao modelo de DW adotado.

## 4. Verificar e encerrar

Use `01_warehouse/sql/04_consulta_redshift.sql` e `sql/07_verificar_incrementais.sql` no Query Editor v2. Opcional: `python runner.py verify`.
Para remover apenas este laboratório: `cd 01_warehouse && terraform plan -destroy && terraform destroy`.
**Cuidado:** o S3 usa `force_destroy=true` e apaga versões durante `destroy`. Não use em produção.

## Limites

Sem garantias de disponibilidade no AWS Academy. Nenhum recurso foi implantado por este pacote.
As verificações documentadas são locais; o caminho integrado (S3/Glue/Redshift) precisa ser testado na conta de destino.
