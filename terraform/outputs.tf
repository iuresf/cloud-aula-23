output "landing_bucket" {
  description = "Bucket versionado para entrada, manifestos, rejeitados, evidencias e temporarios."
  value       = module.landing.bucket_name
}

output "glue_database" {
  value = module.landing.catalog_database_name
}

output "execution_role_arn" {
  description = "Use este ARN no campo IAM Role dos jobs Glue da Aula 23."
  value       = local.execution_role_arn
}

output "redshift_bootstrap_sql" {
  description = "SQL estrutural executado pelo Terraform via Redshift Data API."
  value       = "${path.root}/../warehouse/bootstrap.sql"
}

output "redshift_cluster" { value = module.warehouse.cluster_identifier }
output "glue_job" { value = module.glue_etl.job_name }
output "state_machine_arn" { value = module.orchestration.state_machine_arn }
