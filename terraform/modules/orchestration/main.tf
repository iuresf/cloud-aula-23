resource "aws_sfn_state_machine" "warehouse" {
  name     = "${var.project_name}-pipeline"
  role_arn = var.execution_role_arn
  definition = jsonencode({
    Comment = "Manifesto S3 inicia Glue e publicacao do warehouse"
    StartAt = "IntegrarPublicar"
    States = {
      IntegrarPublicar = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Retry = [{
          ErrorEquals     = ["Glue.ConcurrentRunsExceededException"]
          IntervalSeconds = 15, MaxAttempts = 8, BackoffRate = 1.5
        }]
        Parameters = {
          JobName = var.glue_job_name
          Arguments = {
            "--BUCKET.$"         = "$.detail.bucket.name"
            "--MANIFEST.$"       = "$.detail.object.key"
            "--PUBLICAR_PARCIAL" = "true"
          }
        }
        TimeoutSeconds = 1200
        End            = true
      }
    }
  })
  tags = var.tags
}

resource "aws_cloudwatch_event_rule" "manifest" {
  name = "${var.project_name}-manifest"
  event_pattern = jsonencode({
    source = ["aws.s3"], "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [var.bucket_name] }
      object = { key = [{ wildcard = "manifestos/*/ready.json" }] }
    }
  })
}

resource "aws_cloudwatch_event_target" "warehouse" {
  rule     = aws_cloudwatch_event_rule.manifest.name
  arn      = aws_sfn_state_machine.warehouse.arn
  role_arn = var.execution_role_arn
}
