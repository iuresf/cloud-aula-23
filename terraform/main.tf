provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}
data "aws_vpc" "default" { default = true }

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

locals {
  bucket_name = "${var.project_name}-${data.aws_caller_identity.current.account_id}"
  execution_role_arn = coalesce(
    var.execution_role_arn,
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/LabRole"
  )
  tags = {
    Project   = "Aula23"
    ManagedBy = "Terraform"
    Purpose   = "warehouse-landing"
  }
}

module "landing" {
  source           = "./modules/landing"
  bucket_name      = local.bucket_name
  catalog_database = var.glue_catalog_database_name
  project_name     = var.project_name
  retain_bucket    = var.retain_bucket_on_destroy
  tags             = local.tags
}

module "warehouse" {
  source                    = "./modules/warehouse"
  project_name              = var.project_name
  database_name             = var.redshift_database_name
  master_username           = var.redshift_master_username
  bootstrap_sql_source      = "${path.root}/../warehouse/bootstrap.sql"
  subnet_ids                = data.aws_subnets.default.ids
  execution_role_arn        = local.execution_role_arn
  create_final_snapshot     = var.create_final_snapshot_on_destroy
  final_snapshot_identifier = "${var.project_name}-final"
  tags                      = local.tags
}

module "glue_etl" {
  source             = "./modules/glue_etl"
  project_name       = var.project_name
  bucket_name        = module.landing.bucket_name
  cluster_identifier = module.warehouse.cluster_identifier
  execution_role_arn = local.execution_role_arn
  pipeline_source    = "${path.root}/../warehouse/pipeline.py"
  tags               = local.tags
}

module "orchestration" {
  source             = "./modules/orchestration"
  project_name       = var.project_name
  bucket_name        = module.landing.bucket_name
  glue_job_name      = module.glue_etl.job_name
  execution_role_arn = local.execution_role_arn
  tags               = local.tags
}
