variable "team_name" {
  description = "Domain team name — used in resource naming (e.g. 'payments', 'marketing')."
  type        = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,20}$", var.team_name))
    error_message = "team_name must be lowercase alphanumeric with hyphens, 2-21 chars."
  }
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "kms_key_arn" {
  description = "ARN of the KMS key for S3 and Glue encryption."
  type        = string
}

variable "enable_versioning" {
  description = "Enable S3 versioning on Silver and Gold buckets. Always true in prod."
  type        = bool
  default     = false
}

variable "bronze_retention_days" {
  description = "Days before Bronze objects transition to Glacier IR."
  type        = number
  default     = 90
}

variable "silver_retention_days" {
  description = "Days before Silver objects expire."
  type        = number
  default     = 365
}

variable "gold_retention_days" {
  description = "Days before Gold objects expire."
  type        = number
  default     = 730
}

variable "reader_role_arns" {
  description = "IAM role ARNs (not full ARNs — just role names) to attach the reader policy to."
  type        = list(string)
  default     = []
}

variable "writer_role_arns" {
  description = "IAM role names to attach the writer policy to."
  type        = list(string)
  default     = []
}
