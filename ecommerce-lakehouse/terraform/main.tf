terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    # Values supplied via -backend-config=environments/<env>.tfbackend
    # to avoid hardcoding account-specific details here.
    encrypt = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "ecommerce-lakehouse"
      Environment = var.environment
      Team        = "data-engineering"
      ManagedBy   = "terraform"
    }
  }
}

# ---------------------------------------------------------------------------
# Data Lake (S3 + Lifecycle + Encryption)
# ---------------------------------------------------------------------------

module "s3_data_lake" {
  source = "./modules/s3_data_lake"

  environment    = var.environment
  bucket_prefix  = var.bucket_prefix
  kms_key_arn    = module.kms.key_arn

  bronze_retention_days  = var.bronze_retention_days
  silver_retention_days  = var.silver_retention_days
  gold_retention_days    = var.gold_retention_days
}

# ---------------------------------------------------------------------------
# AWS Glue Data Catalog
# ---------------------------------------------------------------------------

module "glue_catalog" {
  source = "./modules/glue_catalog"

  environment   = var.environment
  s3_bucket_arn = module.s3_data_lake.bucket_arn
  kms_key_arn   = module.kms.key_arn
  database_name = "ecommerce_${var.environment}"
}

# ---------------------------------------------------------------------------
# Redshift Cluster (serving layer)
# ---------------------------------------------------------------------------

module "redshift" {
  source = "./modules/redshift_cluster"

  environment       = var.environment
  cluster_identifier = "ecommerce-dw-${var.environment}"
  node_type         = var.redshift_node_type
  number_of_nodes   = var.redshift_num_nodes
  database_name     = "ecommerce_dw"
  master_username   = var.redshift_master_username
  master_password   = var.redshift_master_password   # In practice: Secrets Manager reference
  kms_key_arn       = module.kms.key_arn
  vpc_id            = var.vpc_id
  subnet_ids        = var.private_subnet_ids
  s3_bucket_arn     = module.s3_data_lake.bucket_arn
}

# ---------------------------------------------------------------------------
# MWAA (Managed Airflow)
# ---------------------------------------------------------------------------

module "mwaa" {
  source = "./modules/mwaa"

  environment         = var.environment
  airflow_env_name    = "ecommerce-airflow-${var.environment}"
  s3_bucket_name      = module.s3_data_lake.bucket_name
  dags_s3_path        = "airflow/dags"
  plugins_s3_path     = "airflow/plugins.zip"
  requirements_s3_path = "airflow/requirements.txt"
  airflow_version     = "2.8.1"
  environment_class   = var.mwaa_environment_class
  max_workers         = var.mwaa_max_workers
  vpc_id              = var.vpc_id
  private_subnet_ids  = var.private_subnet_ids
  kms_key_arn         = module.kms.key_arn
}

# ---------------------------------------------------------------------------
# KMS key for at-rest encryption across all services
# ---------------------------------------------------------------------------

module "kms" {
  source = "./modules/kms"

  environment  = var.environment
  service_name = "ecommerce-lakehouse"
}
