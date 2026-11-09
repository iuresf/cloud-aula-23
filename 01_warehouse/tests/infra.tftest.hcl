mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "111111111111" }
  }
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
  mock_data "aws_redshift_cluster" {
    defaults = { endpoint = "test.redshift.amazonaws.com:5439" }
  }
  mock_resource "aws_cloudwatch_log_group" {
    defaults = { arn = "arn:aws:logs:us-east-1:111111111111:log-group:test" }
  }
  mock_resource "aws_sfn_state_machine" {
    defaults = { arn = "arn:aws:states:us-east-1:111111111111:stateMachine:test" }
  }
  mock_resource "aws_sqs_queue" {
    defaults = { arn = "arn:aws:sqs:us-east-1:111111111111:test" }
  }
}

variables {
  vpc_id                      = "vpc-test"
  redshift_cluster_identifier = "existing-cluster"
  redshift_database           = "aula23"
  redshift_user               = "awsuser"
  redshift_security_group_id  = "sg-test"
  glue_subnet_id              = "subnet-a"
  glue_role_arn               = null
  redshift_s3_role_arn        = null
  sfn_role_arn                = null
  events_role_arn             = null
  bootstrap_via_terraform     = false
}

run "auditoria_sem_provisionar_aws" {
  command = apply
  override_data {
    target = data.aws_subnet.glue
    values = { vpc_id = "vpc-test", availability_zone = "us-east-1a" }
  }
  assert {
    condition     = jsondecode(aws_cloudformation_stack.bucket.template_body).Resources.Bucket.DeletionPolicy == "Retain"
    error_message = "Os originais versionados nao devem ser destruidos automaticamente."
  }
  assert {
    condition     = jsondecode(aws_sfn_state_machine.dw.definition).StartAt == "IdentificarTentativa" && jsondecode(aws_sfn_state_machine.dw.definition).States.RegistrarInicio.Next == "ProcessarLote"
    error_message = "O workflow deve registrar o inicio antes do Glue."
  }
  assert {
    condition     = jsondecode(aws_sfn_state_machine.dw.definition).States.ProcessarLote.Parameters.Arguments["--MANIFEST_VERSION.$"] == "$.detail.object['version-id']" && jsondecode(aws_sfn_state_machine.dw.definition).States.ProcessarLote.Catch[0].Next == "PrepararFalha"
    error_message = "A versao do manifesto e o registro independente de falha sao obrigatorios."
  }
  assert {
    condition     = aws_glue_job.dw.default_arguments["--QUALITY_ALLOW_PARTIAL"] == "false" && length(aws_cloudwatch_metric_alarm.workflow_failures.alarm_actions) == 0
    error_message = "Rejeicoes parciais e notificacoes devem estar desativadas por padrao."
  }
  assert {
    condition     = aws_glue_job.bootstrap.role_arn == "arn:aws:iam::111111111111:role/LabRole" && aws_glue_job.dw.role_arn == "arn:aws:iam::111111111111:role/LabRole" && aws_glue_job.dw.default_arguments["--REDSHIFT_S3_ROLE"] == "arn:aws:iam::111111111111:role/LabRole" && aws_sfn_state_machine.dw.role_arn == "arn:aws:iam::111111111111:role/LabRole" && aws_cloudwatch_event_target.pipeline.role_arn == "arn:aws:iam::111111111111:role/LabRole"
    error_message = "Todos os servicos devem usar a LabRole da conta ativa por padrao."
  }
}

run "roles_explicitas_sobrescrevem_padrao" {
  command = plan
  variables {
    glue_role_arn        = "arn:aws:iam::111111111111:role/custom-glue"
    redshift_s3_role_arn = "arn:aws:iam::111111111111:role/custom-redshift"
    sfn_role_arn         = "arn:aws:iam::111111111111:role/custom-sfn"
    events_role_arn      = "arn:aws:iam::111111111111:role/custom-events"
  }
  assert {
    condition     = aws_glue_job.dw.role_arn == var.glue_role_arn && aws_glue_job.dw.default_arguments["--REDSHIFT_S3_ROLE"] == var.redshift_s3_role_arn && aws_sfn_state_machine.dw.role_arn == var.sfn_role_arn && aws_cloudwatch_event_target.pipeline.role_arn == var.events_role_arn
    error_message = "Roles explicitamente informadas devem substituir o padrao."
  }
}

run "bootstrap_planejado_sem_chamar_aws" {
  command = plan
  variables {
    bootstrap_via_terraform = true
  }
  override_data {
    target = data.aws_subnet.glue
    values = { vpc_id = "vpc-test", availability_zone = "us-east-1a" }
  }
  assert {
    condition     = length(terraform_data.modelo_inicializado) == 1
    error_message = "O bootstrap precisa estar presente por padrao no primeiro apply."
  }
  assert {
    condition     = aws_glue_job.bootstrap.role_arn == aws_glue_job.dw.role_arn
    error_message = "Inicializacao e carga devem usar a mesma role do Glue."
  }
}
