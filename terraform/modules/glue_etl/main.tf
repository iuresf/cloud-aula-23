resource "aws_s3_object" "pipeline" {
  bucket      = var.bucket_name
  key         = "scripts/pipeline.py"
  source      = var.pipeline_source
  source_hash = filesha256(var.pipeline_source)
}

resource "aws_glue_job" "warehouse" {
  name         = "${var.project_name}-etl"
  role_arn     = var.execution_role_arn
  max_capacity = 0.0625
  max_retries  = 0
  timeout      = 15

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${var.bucket_name}/${aws_s3_object.pipeline.key}"
  }

  execution_property { max_concurrent_runs = 1 }

  default_arguments = {
    "--BUCKET"              = var.bucket_name
    "--CLUSTER"             = var.cluster_identifier
    "--MANIFEST"            = "manifestos/lote01/ready.json"
    "--PUBLICAR_PARCIAL"    = "false"
    "--job-bookmark-option" = "job-bookmark-disable"
  }
  tags = var.tags
}
