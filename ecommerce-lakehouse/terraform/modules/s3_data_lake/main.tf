/*
  S3 Data Lake module

  Creates three tiered buckets (Bronze, Silver, Gold) with:
  - SSE-KMS encryption
  - Versioning on Silver/Gold (recovery from bad dbt runs)
  - Lifecycle policies to transition Bronze to Glacier
  - Block public access (no exceptions)
  - Access logging to a dedicated audit bucket
*/

locals {
  bronze_bucket = "${var.bucket_prefix}-datalake-bronze-${var.environment}"
  silver_bucket = "${var.bucket_prefix}-datalake-silver-${var.environment}"
  gold_bucket   = "${var.bucket_prefix}-datalake-gold-${var.environment}"
  audit_bucket  = "${var.bucket_prefix}-datalake-audit-${var.environment}"
}

# ---------------------------------------------------------------------------
# Audit/logging bucket (no lifecycle on this one)
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "audit" {
  bucket        = local.audit_bucket
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_public_access_block" "audit" {
  bucket                  = aws_s3_bucket.audit.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "audit" {
  bucket = aws_s3_bucket.audit.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

# ---------------------------------------------------------------------------
# Bronze bucket
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "bronze" {
  bucket        = local.bronze_bucket
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_logging" "bronze" {
  bucket        = aws_s3_bucket.bronze.id
  target_bucket = aws_s3_bucket.audit.id
  target_prefix = "bronze-access-logs/"
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "archive-old-bronze"
    status = "Enabled"

    transition {
      days          = var.bronze_retention_days
      storage_class = "GLACIER_IR"
    }

    expiration {
      days = 1825    # 5 years — regulatory minimum for audit trail
    }
  }
}

# ---------------------------------------------------------------------------
# Silver bucket (versioning enabled for point-in-time recovery)
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "silver" {
  bucket        = local.silver_bucket
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    id     = "expire-old-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "expire-silver-data"
    status = "Enabled"

    expiration {
      days = var.silver_retention_days
    }
  }
}

# ---------------------------------------------------------------------------
# Gold bucket (versioning enabled)
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "gold" {
  bucket        = local.gold_bucket
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}
