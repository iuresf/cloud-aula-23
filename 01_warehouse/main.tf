terraform {
  required_version = ">= 1.5, < 2.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}
provider "aws" { region = var.region }
data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_redshift_cluster" "existing" { cluster_identifier = var.redshift_cluster_identifier }
data "aws_subnet" "glue" { id = var.glue_subnet_id }
locals {
  bucket_name          = "${var.project}-${data.aws_caller_identity.current.account_id}-${var.region}"
  lab_role_arn         = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/LabRole"
  glue_role_arn        = var.glue_role_arn != null ? var.glue_role_arn : local.lab_role_arn
  redshift_s3_role_arn = var.redshift_s3_role_arn != null ? var.redshift_s3_role_arn : local.lab_role_arn
  sfn_role_arn         = var.sfn_role_arn != null ? var.sfn_role_arn : local.lab_role_arn
  events_role_arn      = var.events_role_arn != null ? var.events_role_arn : local.lab_role_arn
  assets = {
    "scripts/warehouse_etl.py"      = "jobs/warehouse_etl.py"
    "scripts/warehouse_core.py"     = "jobs/warehouse_core.py"
    "scripts/warehouse_spark.py"    = "jobs/warehouse_spark.py"
    "scripts/inicializar_modelo.py" = "jobs/inicializar_modelo.py"
    "sql/01_modelo.sql"             = "sql/01_modelo.sql"
    "sql/05_publicar.sql"           = "sql/05_publicar.sql"
  }
}

# O job de bootstrap usa a MESMA role do job Spark: o proprietario dos
# objetos no Redshift e o mesmo principal que fara a carga.
resource "aws_glue_job" "bootstrap" {
  name         = "${var.project}-inicializar-modelo"
  role_arn     = local.glue_role_arn
  max_capacity = 0.0625
  max_retries  = 0
  timeout      = 15
  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${local.bucket_name}/scripts/inicializar_modelo.py"
  }
  default_arguments = {
    "--ASSET_BUCKET"      = local.bucket_name
    "--REDSHIFT_CLUSTER"  = data.aws_redshift_cluster.existing.cluster_identifier
    "--REDSHIFT_DATABASE" = var.redshift_database
    "--REDSHIFT_USER"     = var.redshift_user
    "--REGION"            = var.region
  }
  depends_on = [aws_s3_object.assets]
}

# Executa e espera o bootstrap durante o apply, apos workgroup e assets.
# Reexecuta em mudancas de SQL/script, workgroup ou role, sem substituir dados.
resource "terraform_data" "modelo_inicializado" {
  count = var.bootstrap_via_terraform ? 1 : 0
  triggers_replace = {
    job             = aws_glue_job.bootstrap.name
    workgroup       = data.aws_redshift_cluster.existing.id
    role            = local.glue_role_arn
    script_sha256   = filesha256("${path.module}/jobs/inicializar_modelo.py")
    modelo_sha256   = filesha256("${path.module}/sql/01_modelo.sql")
    publicar_sha256 = filesha256("${path.module}/sql/05_publicar.sql")
  }
  provisioner "local-exec" {
    working_dir = path.module
    command     = "python3 scripts/esperar_bootstrap.py"
    environment = {
      A23_BOOTSTRAP_JOB     = aws_glue_job.bootstrap.name
      A23_REGION            = var.region
      A23_BOOTSTRAP_TIMEOUT = "1800"
    }
  }
  depends_on = [aws_s3_object.assets]
}
resource "aws_cloudformation_stack" "bucket" {
  name = "${var.project}-bucket"
  template_body = jsonencode({
    Resources = { Bucket = {
      Type                = "AWS::S3::Bucket"
      DeletionPolicy      = "Retain"
      UpdateReplacePolicy = "Retain"
      Properties = {
        BucketName                     = local.bucket_name
        VersioningConfiguration        = { Status = "Enabled" }
        PublicAccessBlockConfiguration = { BlockPublicAcls = true, BlockPublicPolicy = true, IgnorePublicAcls = true, RestrictPublicBuckets = true }
        BucketEncryption               = { ServerSideEncryptionConfiguration = [{ ServerSideEncryptionByDefault = { SSEAlgorithm = "AES256" } }] }
        NotificationConfiguration      = { EventBridgeConfiguration = { EventBridgeEnabled = true } }
        LifecycleConfiguration         = { Rules = [{ Id = "temporarios-do-conector", Status = "Enabled", Prefix = "temporario/", ExpirationInDays = 2, NoncurrentVersionExpiration = { NoncurrentDays = 2 } }] }
      }
    } }
  })
}
resource "aws_s3_object" "assets" {
  for_each   = local.assets
  bucket     = local.bucket_name
  key        = each.key
  source     = "${path.module}/${each.value}"
  etag       = filemd5("${path.module}/${each.value}")
  depends_on = [aws_cloudformation_stack.bucket]
}
resource "aws_security_group" "glue" {
  name_prefix = "${var.project}-glue-"
  description = "Workers Glue e acesso privado ao Redshift"
  vpc_id      = var.vpc_id
}
resource "aws_vpc_security_group_ingress_rule" "glue_self" {
  security_group_id            = aws_security_group.glue.id
  referenced_security_group_id = aws_security_group.glue.id
  ip_protocol                  = "tcp"
  from_port                    = 0
  to_port                      = 65535
}
resource "aws_vpc_security_group_egress_rule" "glue_self" {
  security_group_id            = aws_security_group.glue.id
  referenced_security_group_id = aws_security_group.glue.id
  ip_protocol                  = "tcp"
  from_port                    = 0
  to_port                      = 65535
}
resource "aws_vpc_security_group_egress_rule" "glue_https" {
  security_group_id = aws_security_group.glue.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}
resource "aws_vpc_security_group_egress_rule" "glue_redshift" {
  security_group_id            = aws_security_group.glue.id
  referenced_security_group_id = var.redshift_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5439
  to_port                      = 5439
}
resource "aws_vpc_security_group_ingress_rule" "redshift_glue" {
  security_group_id            = var.redshift_security_group_id
  referenced_security_group_id = aws_security_group.glue.id
  ip_protocol                  = "tcp"
  from_port                    = 5439
  to_port                      = 5439
}
# Opcional: sem NAT, crie estes endpoints. Nao se cria NAT Gateway nem VPC.
resource "aws_security_group" "endpoints" {
  count       = var.create_private_endpoints ? 1 : 0
  name_prefix = "${var.project}-endpoints-"
  vpc_id      = var.vpc_id
}
resource "aws_vpc_security_group_ingress_rule" "endpoint_https" {
  count                        = var.create_private_endpoints ? 1 : 0
  security_group_id            = aws_security_group.endpoints[0].id
  referenced_security_group_id = aws_security_group.glue.id
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
}
resource "aws_vpc_endpoint" "s3" {
  count             = var.create_private_endpoints ? 1 : 0
  vpc_id            = var.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = var.route_table_ids
  lifecycle {
    precondition {
      condition     = length(var.route_table_ids) > 0
      error_message = "Informe as tabelas de rotas das sub-redes Glue e Redshift para o endpoint S3."
    }
  }
}
resource "aws_vpc_endpoint" "api" {
  for_each            = var.create_private_endpoints ? toset(["glue", "logs", "redshift-data", "redshift"]) : toset([])
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.region}.${each.value}"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [var.glue_subnet_id]
  security_group_ids  = [aws_security_group.endpoints[0].id]
}
resource "aws_glue_connection" "network" {
  name            = "${var.project}-network"
  connection_type = "NETWORK"
  physical_connection_requirements {
    availability_zone      = data.aws_subnet.glue.availability_zone
    subnet_id              = var.glue_subnet_id
    security_group_id_list = [aws_security_group.glue.id]
  }
}
resource "aws_glue_job" "dw" {
  name                    = "${var.project}-etl"
  role_arn                = local.glue_role_arn
  glue_version            = "5.0"
  worker_type             = "G.1X"
  number_of_workers       = 2
  timeout                 = 20
  max_retries             = 0
  job_run_queuing_enabled = true
  connections             = [aws_glue_connection.network.name]
  execution_property { max_concurrent_runs = 1 }
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.bucket_name}/scripts/warehouse_etl.py"
  }
  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--custom-logGroup-prefix"           = "/aws-glue/jobs/${var.project}"
    "--QUALITY_ALLOW_PARTIAL"            = tostring(var.quality_allow_partial)
    "--extra-py-files"                   = "s3://${local.bucket_name}/scripts/warehouse_core.py,s3://${local.bucket_name}/scripts/warehouse_spark.py"
    "--TempDir"                          = "s3://${local.bucket_name}/temporario/glue/"
    "--ASSET_BUCKET"                     = local.bucket_name
    "--REDSHIFT_CLUSTER"                 = data.aws_redshift_cluster.existing.cluster_identifier
    "--REDSHIFT_DATABASE"                = var.redshift_database
    "--REDSHIFT_USER"                    = var.redshift_user
    "--REDSHIFT_HOST"                    = split(":", data.aws_redshift_cluster.existing.endpoint)[0]
    "--REDSHIFT_S3_ROLE"                 = local.redshift_s3_role_arn
    "--REGION"                           = var.region
  }
  depends_on = [aws_s3_object.assets, aws_vpc_endpoint.s3, aws_vpc_endpoint.api]
}
resource "aws_cloudwatch_log_group" "workflow" {
  name              = "/aws/vendedlogs/states/${var.project}"
  retention_in_days = var.log_retention_days
}
resource "aws_cloudwatch_log_group" "glue" {
  for_each          = toset(["output", "error"])
  name              = "/aws-glue/jobs/${var.project}/${each.value}"
  retention_in_days = var.log_retention_days
}
resource "aws_sfn_state_machine" "dw" {
  name       = "${var.project}-pipeline"
  role_arn   = local.sfn_role_arn
  type       = "STANDARD"
  depends_on = [terraform_data.modelo_inicializado]
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.workflow.arn}:*"
    include_execution_data = false
    level                  = "ERROR"
  }
  definition = jsonencode({
    Comment        = "Manifesto versionado inicia ETL e publicacao atomica no Redshift"
    StartAt        = "IdentificarTentativa"
    TimeoutSeconds = 3600
    States = {
      IdentificarTentativa = {
        Type = "Pass"
        Parameters = {
          detail = {
            bucket = { "name.$" = "$.detail.bucket.name" }
            object = {
              "key.$"        = "$.detail.object.key"
              "version-id.$" = "$.detail.object['version-id']"
            }
          }
          "attempt_id.$"         = "States.UUID()"
          "step_execution_arn.$" = "$$.Execution.Id"
          "inicio.$"             = "$$.State.EnteredTime"
          "lote_id.$"            = "States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 1)"
        }
        Next = "RegistrarInicio"
      }
      RegistrarInicio = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::aws-sdk:s3:putObject"
        Parameters = {
          Bucket      = local.bucket_name
          "Key.$"     = "States.Format('auditoria/tentativas/{}/workflow_inicio.json', $.attempt_id)"
          "Body.$"    = "States.JsonToString($)"
          ContentType = "application/json"
        }
        ResultPath = null
        Next       = "ProcessarLote"
      }
      ProcessarLote = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.dw.name
          Arguments = {
            "--MANIFEST_BUCKET.$"    = "$.detail.bucket.name"
            "--MANIFEST_KEY.$"       = "$.detail.object.key"
            "--MANIFEST_VERSION.$"   = "$.detail.object['version-id']"
            "--ATTEMPT_ID.$"         = "$.attempt_id"
            "--STEP_EXECUTION_ARN.$" = "$.step_execution_arn"
          }
        }
        Retry = [{
          ErrorEquals     = ["Glue.ConcurrentRunsExceededException", "Glue.InternalServiceException", "Glue.OperationTimeoutException"]
          IntervalSeconds = 30
          MaxAttempts     = 3
          BackoffRate     = 2
        }]
        ResultSelector = { "glue_job_run_id.$" = "$.Id" }
        ResultPath     = "$.glue"
        Catch          = [{ ErrorEquals = ["States.ALL"], ResultPath = "$.falha", Next = "PrepararFalha" }]
        Next           = "PrepararConclusao"
      }
      PrepararConclusao = {
        Type = "Pass"
        Parameters = {
          "attempt_id.$"         = "$.attempt_id"
          "lote_id.$"            = "$.lote_id"
          "step_execution_arn.$" = "$.step_execution_arn"
          "glue_job_run_id.$"    = "$.glue.glue_job_run_id"
          "manifesto_key.$"      = "$.detail.object.key"
          "manifesto_version.$"  = "$.detail.object['version-id']"
          "termino.$"            = "$$.State.EnteredTime"
          status                 = "GLUE_CONCLUIDO"
        }
        Next = "RegistrarConclusao"
      }
      RegistrarConclusao = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::aws-sdk:s3:putObject"
        Parameters = {
          Bucket      = local.bucket_name
          "Key.$"     = "States.Format('auditoria/tentativas/{}/workflow_conclusao.json', $.attempt_id)"
          "Body.$"    = "States.JsonToString($)"
          ContentType = "application/json"
        }
        ResultPath = null
        Next       = "LotePublicado"
      }
      PrepararFalha = {
        Type = "Pass"
        Parameters = {
          "attempt_id.$"         = "$.attempt_id"
          "lote_id.$"            = "$.lote_id"
          "step_execution_arn.$" = "$.step_execution_arn"
          "manifesto_key.$"      = "$.detail.object.key"
          "manifesto_version.$"  = "$.detail.object['version-id']"
          "erro_codigo.$"        = "$.falha.Error"
          "termino.$"            = "$$.State.EnteredTime"
          status                 = "FALHA_WORKFLOW"
        }
        Next = "RegistrarFalha"
      }
      RegistrarFalha = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::aws-sdk:s3:putObject"
        Parameters = {
          Bucket      = local.bucket_name
          "Key.$"     = "States.Format('auditoria/tentativas/{}/workflow_falha.json', $.attempt_id)"
          "Body.$"    = "States.JsonToString($)"
          ContentType = "application/json"
        }
        ResultPath = null
        Next       = "FalhaDoLote"
      }
      LotePublicado = { Type = "Succeed" }
      FalhaDoLote   = { Type = "Fail", Error = "FalhaPipelineWarehouse", Cause = "Consulte execution history, Glue e rejeitados no S3. Reenvie o mesmo manifesto para repetir com seguranca." }
    }
  })
}
resource "aws_cloudwatch_event_rule" "ready" {
  name = "${var.project}-manifesto"
  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [local.bucket_name] }
      object = { key = [{ wildcard = "manifestos/*/ready.json" }] }
    }
  })
  depends_on = [aws_cloudformation_stack.bucket]
}
resource "aws_sqs_queue" "delivery_failures" {
  name                      = "${var.project}-eventos-nao-entregues"
  sqs_managed_sse_enabled   = true
  message_retention_seconds = 1209600
}
resource "aws_sqs_queue_policy" "delivery_failures" {
  queue_url = aws_sqs_queue.delivery_failures.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.delivery_failures.arn
      Condition = { ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.ready.arn } }
    }]
  })
}
resource "aws_cloudwatch_event_target" "pipeline" {
  rule     = aws_cloudwatch_event_rule.ready.name
  arn      = aws_sfn_state_machine.dw.arn
  role_arn = local.events_role_arn
  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 3
  }
  dead_letter_config { arn = aws_sqs_queue.delivery_failures.arn }
  depends_on = [aws_sqs_queue_policy.delivery_failures]
}
