output "bronze_bucket_name" { value = aws_s3_bucket.bronze.bucket }
output "silver_bucket_name" { value = aws_s3_bucket.silver.bucket }
output "gold_bucket_name"   { value = aws_s3_bucket.gold.bucket }

output "bronze_bucket_arn"  { value = aws_s3_bucket.bronze.arn }
output "silver_bucket_arn"  { value = aws_s3_bucket.silver.arn }
output "gold_bucket_arn"    { value = aws_s3_bucket.gold.arn }

output "glue_bronze_database" { value = aws_glue_catalog_database.bronze.name }
output "glue_silver_database" { value = aws_glue_catalog_database.silver.name }
output "glue_gold_database"   { value = aws_glue_catalog_database.gold.name }

output "reader_policy_arn"  { value = aws_iam_policy.reader.arn }
output "writer_policy_arn"  { value = aws_iam_policy.writer.arn }
