output "region" { value = var.region }
output "redshift_cluster" { value = data.aws_redshift_cluster.existing.cluster_identifier }
output "redshift_database" { value = var.redshift_database }
output "ingestion_bucket" { value = local.bucket_name }
output "glue_job" { value = aws_glue_job.dw.name }
output "state_machine_arn" { value = aws_sfn_state_machine.dw.arn }
output "delivery_failures_queue_url" { value = aws_sqs_queue.delivery_failures.url }
output "glue_security_group_id" { value = aws_security_group.glue.id }
output "redshift_security_group_id" { value = var.redshift_security_group_id }
output "permissions_scope" {
  value = {
    bucket_arn       = "arn:${data.aws_partition.current.partition}:s3:::${local.bucket_name}"
    cluster_arn      = data.aws_redshift_cluster.existing.arn
    job_arn          = aws_glue_job.dw.arn
    state_machine    = aws_sfn_state_machine.dw.arn
    redshift_s3_role = local.redshift_s3_role_arn
  }
}
output "audit_prefix" {
  description = "Eventos externos por tentativa; persistem independentemente do commit Redshift."
  value       = "s3://${local.bucket_name}/auditoria/tentativas/"
}

output "quality_alarm_name" {
  value = aws_cloudwatch_metric_alarm.quality_rejections.alarm_name
}

output "bootstrap_job_name" {
  description = "Job que instala e verifica tabelas e procedure durante terraform apply."
  value       = aws_glue_job.bootstrap.name
}
