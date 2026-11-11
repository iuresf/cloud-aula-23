resource "aws_redshift_subnet_group" "warehouse" {
  name       = var.project_name
  subnet_ids = var.subnet_ids
  tags       = var.tags
}

ephemeral "random_password" "admin" {
  length  = 32
  special = false
}

resource "aws_redshift_cluster" "warehouse" {
  cluster_identifier         = var.project_name
  database_name              = var.database_name
  master_username            = var.master_username
  master_password_wo         = ephemeral.random_password.admin.result
  master_password_wo_version = 1
  node_type                  = "ra3.large"
  cluster_type               = "single-node"
  cluster_subnet_group_name  = aws_redshift_subnet_group.warehouse.name
  publicly_accessible        = false
  encrypted                  = true
  iam_roles                  = [var.execution_role_arn]
  skip_final_snapshot        = !var.create_final_snapshot
  final_snapshot_identifier  = var.create_final_snapshot ? var.final_snapshot_identifier : null
  tags                       = var.tags
}

# O schema e as tabelas fazem parte da infraestrutura. Este recurso so e
# reexecutado quando o SQL estrutural for alterado.
resource "aws_redshiftdata_statement" "bootstrap" {
  cluster_identifier = aws_redshift_cluster.warehouse.cluster_identifier
  database           = var.database_name
  db_user            = var.master_username
  statement_name     = "${var.project_name}-bootstrap"
  sql                = file(var.bootstrap_sql_source)

  depends_on = [aws_redshift_cluster.warehouse]
}
