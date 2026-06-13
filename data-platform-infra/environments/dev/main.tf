/*
  Dev environment — cost-optimised for engineering experimentation.

  Differences from prod:
  - Single-AZ (no multi-AZ overhead)
  - Smallest viable instance types
  - No deletion protection
  - 1-day backup retention
  - force_destroy = true on S3 (easy cleanup)
*/

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {
    bucket         = "acme-terraform-state-dev"
    key            = "data-platform/dev/terraform.tfstate"
    region         = "eu-west-1"
    encrypt        = true
    dynamodb_table = "terraform-state-locks"
  }
}

provider "aws" {
  region = "eu-west-1"
  default_tags {
    tags = {
      Environment = "dev"
      Team        = "data-engineering"
      ManagedBy   = "terraform"
      CostCenter  = "data-platform"
    }
  }
}

locals { environment = "dev" }

module "networking" {
  source      = "../../modules/networking"
  environment = local.environment
  vpc_cidr    = "10.1.0.0/16"
  az_count    = 2
}

module "kms" {
  source       = "../../modules/kms"
  environment  = local.environment
  service_name = "data-platform"
}

module "ecommerce_lake" {
  source      = "../../modules/data-lake"
  team_name   = "ecommerce"
  environment = local.environment
  kms_key_arn = module.kms.key_arn

  enable_versioning     = false
  bronze_retention_days = 30
  silver_retention_days = 30
  gold_retention_days   = 30
}

module "redshift" {
  source = "../../modules/data-warehouse"

  environment        = local.environment
  cluster_identifier = "analytics-dev"
  node_type          = "ra3.xlplus"
  number_of_nodes    = 1
  database_name      = "analytics"
  kms_key_arn        = module.kms.key_arn
  vpc_id             = module.networking.vpc_id
  subnet_ids         = module.networking.private_subnet_ids

  deletion_protection = false
  backup_retention    = 1
}

module "mwaa" {
  source = "../../modules/orchestration"

  environment        = local.environment
  airflow_env_name   = "data-platform-dev"
  s3_bucket_name     = module.ecommerce_lake.silver_bucket_name
  environment_class  = "mw1.small"
  max_workers        = 3
  min_workers        = 1
  vpc_id             = module.networking.vpc_id
  private_subnet_ids = module.networking.private_subnet_ids
  kms_key_arn        = module.kms.key_arn
}
