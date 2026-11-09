variable "region" {
  description = "Regiao AWS Academy que recebera os recursos."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefixo DNS dos recursos."
  type        = string
  default     = "aula23-dw"
}

variable "glue_catalog_database_name" {
  description = "Catalogo para os dados de aterrissagem; nao representa o Data Mart Redshift."
  type        = string
  default     = "aula23_landing"
}

variable "execution_role_arn" {
  description = "Role existente usada pelo Glue, Redshift, Step Functions e EventBridge. Padrao: LabRole da conta ativa."
  type        = string
  default     = null
  nullable    = true
}

variable "redshift_database_name" {
  description = "Banco inicial do cluster Redshift didatico."
  type        = string
  default     = "aula23"
}

variable "redshift_master_username" {
  description = "Usuario administrador inicial do Redshift."
  type        = string
  default     = "awsuser"
}

variable "retain_bucket_on_destroy" {
  description = "Preserva o bucket e seus dados quando o Terraform e destruido. Para reset completo, limpe as versoes S3 manualmente e desative esta opcao."
  type        = bool
  default     = true
}

variable "create_final_snapshot_on_destroy" {
  description = "Cria snapshot final do Redshift no destroy. Em aulas repetiveis, deixe false para nao acumular snapshots e bloquear o mesmo nome."
  type        = bool
  default     = false
}
