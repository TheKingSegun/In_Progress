/*
  Production environment

  All modules are deployed with HA settings:
  - Multi-AZ across 3 availability zones
  - Deletion protection enabled
  - Enhanced monitoring
  - Full backup retention (35 days)

  IMPORTANT: Terraform applies to prod require two-engineer approval via
  the GitHub Actions workflow gate. Never apply directly from local.
*/

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {
    bucket         = "acme-terraform-state-prod"
    key            = "data-platform/prod/terraform.tfstate"
    region         = "eu-west-1"
    encrypt        = true
    dynamodb_table = "terraform-state-locks"
  }
}

provider "aws" {
  region = "eu-west-1"
  default_tags {
    tags = {
      Environment = "prod"
      Team        = "data-engineering"
      ManagedBy   = "terraform"
      CostCenter  = "data-platform"
    }
  }
}

locals {
  environment = "prod"
}

module "networking" {
  source      = "../../modules/networking"
  environment = local.environment
  vpc_cidr    = "10.0.0.0/16"
  az_count    = 3
}

module "kms" {
  source       = "../../modules/kms"
  environment  = local.environment
  service_name = "data-platform"
}

# ---------------------------------------------------------------------------
# Domain data lakes
# ---------------------------------------------------------------------------

module "ecommerce_lake" {
  source      = "../../modules/data-lake"
  team_name   = "ecommerce"
  environment = local.environment
  kms_key_arn = module.kms.key_arn

  enable_versioning     = true
  bronze_retention_days = 90
  silver_retention_days = 365
  gold_retention_days   = 730

  reader_role_arns = [module.redshift.spectrum_role_name]
  writer_role_arns = [module.mwaa.execution_role_name]
}

module "payments_lake" {
  source      = "../../modules/data-lake"
  team_name   = "payments"
  environment = local.environment
  kms_key_arn = module.kms.key_arn

  enable_versioning     = true
  bronze_retention_days = 365    # Longer retention for financial audit trail
  silver_retention_days = 1825   # 5 years for PCI-DSS compliance
  gold_retention_days   = 1825

  reader_role_arns = [module.redshift.spectrum_role_name]
  writer_role_arns = [module.mwaa.execution_role_name]
}

# ---------------------------------------------------------------------------
# Redshift (analytics warehouse)
# ---------------------------------------------------------------------------

module "redshift" {
  source = "../../modules/data-warehouse"

  environment        = local.environment
  cluster_identifier = "analytics-prod"
  node_type          = "ra3.4xlarge"
  number_of_nodes    = 3
  database_name      = "analytics"
  kms_key_arn        = module.kms.key_arn
  vpc_id             = module.networking.vpc_id
  subnet_ids         = module.networking.private_subnet_ids

  deletion_protection  = true
  backup_retention     = 35
  maintenance_window   = "sun:03:00-sun:05:00"
  preferred_snapshot_window = "01:00-03:00"
}

# ---------------------------------------------------------------------------
# MWAA (Airflow)
# ---------------------------------------------------------------------------

module "mwaa" {
  source = "../../modules/orchestration"

  environment          = local.environment
  airflow_env_name     = "data-platform-prod"
  s3_bucket_name       = module.ecommerce_lake.silver_bucket_name
  environment_class    = "mw1.large"
  max_workers          = 20
  min_workers          = 2
  vpc_id               = module.networking.vpc_id
  private_subnet_ids   = module.networking.private_subnet_ids
  kms_key_arn          = module.kms.key_arn
}

# ---------------------------------------------------------------------------
# MSK (Kafka — for streaming pipelines)
# ---------------------------------------------------------------------------

module "kafka" {
  source = "../../modules/streaming"

  environment          = local.environment
  broker_instance_type = "kafka.m5.large"
  broker_count         = 3
  broker_storage_gb    = 500
  kms_key_arn          = module.kms.key_arn
  vpc_id               = module.networking.vpc_id
  private_subnet_ids   = module.networking.private_subnet_ids
  vpc_cidr             = module.networking.vpc_cidr
  log_bucket_name      = module.ecommerce_lake.bronze_bucket_name
}
