output "bucket_name" {
  description = "Name of the primary data lake bucket (used as the MWAA DAG bucket)."
  value       = aws_s3_bucket.silver.bucket
}

output "bucket_arn" {
  description = "ARN of the Silver bucket — used for IAM policy attachments."
  value       = aws_s3_bucket.silver.arn
}

output "bronze_bucket_name" { value = aws_s3_bucket.bronze.bucket }
output "silver_bucket_name" { value = aws_s3_bucket.silver.bucket }
output "gold_bucket_name"   { value = aws_s3_bucket.gold.bucket }

output "bronze_bucket_arn" { value = aws_s3_bucket.bronze.arn }
output "silver_bucket_arn" { value = aws_s3_bucket.silver.arn }
output "gold_bucket_arn"   { value = aws_s3_bucket.gold.arn }
