variable "region" {
  type    = string
  default = "us-east-1"
}
variable "bootstrap_via_terraform" {
  type        = bool
  default     = true
  description = "Instala e verifica o modelo no apply; false apenas para testes com provider simulado."
}
variable "log_retention_days" {
  type        = number
  default     = 30
  description = "Retencao, em dias, dos grupos proprios Glue e Step Functions."
  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653], var.log_retention_days)
    error_message = "Informe um prazo de retencao aceito pelo CloudWatch Logs."
  }
}
variable "quality_allow_partial" {
  type        = bool
  default     = false
  description = "Permite rejeicao parcial de linhas invalidas; conflitos e falhas referenciais sempre bloqueiam."
}
variable "alarm_action_arns" {
  type        = list(string)
  default     = []
  description = "ARNs de acoes de alarme existentes e autorizadas; vazio nao envia notificacoes."
}
variable "project" {
  type    = string
  default = "a23-dw"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,25}$", var.project))
    error_message = "Use 3 a 26 caracteres minusculos, numeros ou hifen."
  }
}
variable "vpc_id" { type = string }
variable "redshift_cluster_identifier" { type = string }
variable "redshift_database" { type = string }
variable "redshift_user" { type = string }
variable "redshift_security_group_id" { type = string }
variable "glue_subnet_id" {
  type        = string
  description = "Subnet privada com IPs livres e acesso S3/APIs via NAT ou endpoints."
}
variable "glue_role_arn" {
  type        = string
  default     = null
  description = "Opcional: role Glue existente. Sem valor, usa LabRole da conta ativa."
}
variable "redshift_s3_role_arn" {
  type        = string
  default     = null
  description = "Opcional: role existente associada ao cluster para COPY. Sem valor, usa LabRole da conta ativa."
}
variable "sfn_role_arn" {
  type        = string
  default     = null
  description = "Opcional: role Step Functions existente. Sem valor, usa LabRole da conta ativa."
}
variable "events_role_arn" {
  type        = string
  default     = null
  description = "Opcional: role EventBridge existente. Sem valor, usa LabRole da conta ativa."
}
variable "create_private_endpoints" {
  type        = bool
  default     = false
  description = "true cria S3 Gateway e 4 Interface endpoints. false exige NAT ou endpoints ja existentes."
}
variable "route_table_ids" {
  type        = list(string)
  default     = []
  description = "Se criar endpoints: rotas de TODAS as sub-redes Glue e Redshift usadas no COPY."
}
