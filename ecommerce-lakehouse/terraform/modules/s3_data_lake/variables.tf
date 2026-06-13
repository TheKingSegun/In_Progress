variable "environment"           { type = string }
variable "bucket_prefix"         { type = string }
variable "kms_key_arn"           { type = string }
variable "bronze_retention_days" { type = number }
variable "silver_retention_days" { type = number }
variable "gold_retention_days"   { type = number }
