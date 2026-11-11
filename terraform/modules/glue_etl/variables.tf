variable "project_name" { type = string }
variable "bucket_name" { type = string }
variable "cluster_identifier" { type = string }
variable "execution_role_arn" { type = string }
variable "pipeline_source" { type = string }
variable "tags" { type = map(string) }
