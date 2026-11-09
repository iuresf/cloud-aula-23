resource "aws_cloudformation_stack" "bucket" {
  name = "${var.project_name}-landing"
  tags = var.tags

  # CloudFormation evita a leitura de Object Lock negada em algumas contas Academy.
  template_body = jsonencode({
    AWSTemplateFormatVersion = "2010-09-09"
    Resources = {
      LandingBucket = merge({
        Type                = "AWS::S3::Bucket"
        UpdateReplacePolicy = var.retain_bucket ? "Retain" : "Delete"
        Properties = {
          BucketName                = var.bucket_name
          NotificationConfiguration = { EventBridgeConfiguration = { EventBridgeEnabled = true } }
          VersioningConfiguration   = { Status = "Enabled" }
          PublicAccessBlockConfiguration = {
            BlockPublicAcls  = true, BlockPublicPolicy = true,
            IgnorePublicAcls = true, RestrictPublicBuckets = true
          }
          BucketEncryption = {
            ServerSideEncryptionConfiguration = [{
              ServerSideEncryptionByDefault = { SSEAlgorithm = "AES256" }
            }]
          }
        }
      }, var.retain_bucket ? { DeletionPolicy = "Retain" } : {})
    }
    Outputs = { BucketName = { Value = { Ref = "LandingBucket" } } }
  })
}

resource "aws_glue_catalog_database" "landing" {
  name        = var.catalog_database
  description = "Landing zone Aula 23; nao cadastrar rejeitados ou evidencias como fatos."
}
