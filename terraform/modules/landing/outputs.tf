output "bucket_name" { value = aws_cloudformation_stack.bucket.outputs["BucketName"] }
output "catalog_database_name" { value = aws_glue_catalog_database.landing.name }
