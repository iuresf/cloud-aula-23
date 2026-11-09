resource "aws_cloudwatch_log_metric_filter" "quality_rejections" {
  name           = "${var.project}-registros-rejeitados"
  log_group_name = aws_cloudwatch_log_group.glue["output"].name
  pattern        = "{ $.status = \"REJEICOES\" }"
  metric_transformation {
    name      = "${var.project}-QualityRejectionEvents"
    namespace = "Aula23/Warehouse"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "workflow_failures" {
  alarm_name          = "${var.project}-workflow-falhas"
  alarm_description   = "Falha de execucao Step Functions; verificar auditoria e Glue."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  dimensions          = { StateMachineArn = aws_sfn_state_machine.dw.arn }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_action_arns
}

resource "aws_cloudwatch_metric_alarm" "workflow_timeouts" {
  alarm_name          = "${var.project}-workflow-timeout"
  alarm_description   = "Step Functions ultrapassou o tempo limite."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsTimedOut"
  dimensions          = { StateMachineArn = aws_sfn_state_machine.dw.arn }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_action_arns
}

resource "aws_cloudwatch_metric_alarm" "quality_rejections" {
  alarm_name          = "${var.project}-qualidade-rejeicoes"
  alarm_description   = "Uma ou mais ocorrencias de rejeicao de qualidade no Glue."
  namespace           = "Aula23/Warehouse"
  metric_name         = "${var.project}-QualityRejectionEvents"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_action_arns
}
