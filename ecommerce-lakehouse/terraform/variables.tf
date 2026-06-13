variable "aws_region" {
  description = "AWS region to deploy all resources."
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Deployment environment: dev | staging | prod."
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "bucket_prefix" {
  description = "Prefix for S3 bucket names, e.g. 'acme-corp'."
  type        = string
}

variable "bronze_retention_days" {
  description = "Days before Bronze objects transition to Glacier. Raw data is immutable but rarely accessed after 90d."
  type        = number
  default     = 90
}

variable "silver_retention_days" {
  description = "Days before Silver objects are deleted. Silver data is rebuilt from Bronze when needed."
  type        = number
  default     = 365
}

variable "gold_retention_days" {
  description = "Days before Gold objects are deleted. Gold is small and cheap to keep."
  type        = number
  default     = 730
}

variable "redshift_node_type" {
  description = "Redshift node type. ra3.xlplus for dev/staging, ra3.4xlarge for prod."
  type        = string
  default     = "ra3.xlplus"
}

variable "redshift_num_nodes" {
  description = "Number of Redshift nodes. 1 for dev, 2+ for prod."
  type        = number
  default     = 1
}

variable "redshift_master_username" {
  description = "Redshift master username (stored in Secrets Manager in prod)."
  type        = string
  sensitive   = true
}

variable "redshift_master_password" {
  description = "Redshift master password (stored in Secrets Manager in prod)."
  type        = string
  sensitive   = true
}

variable "vpc_id" {
  description = "VPC ID where compute resources are deployed."
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for EMR, Redshift, and MWAA."
  type        = list(string)
}

variable "mwaa_environment_class" {
  description = "MWAA environment class (mw1.small | mw1.medium | mw1.large)."
  type        = string
  default     = "mw1.medium"
}

variable "mwaa_max_workers" {
  description = "Maximum number of MWAA workers."
  type        = number
  default     = 5
}
