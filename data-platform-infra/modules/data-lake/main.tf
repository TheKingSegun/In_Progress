/*
  Data Lake module — reusable, opinionated S3 + Glue setup.

  Design decisions:
  - Separate buckets per layer (Bronze/Silver/Gold) rather than prefixes.
    This allows independent lifecycle policies, IAM boundaries, and
    clearer cost attribution per layer.
  - KMS bucket key enabled on all buckets — reduces KMS API calls by ~99%
    for high-volume writes (significant cost saving at scale).
  - Glue catalog database created automatically — crawlers can be added
    as a separate module when needed.
  - IAM policy is resource-specific — no wildcard S3 permissions.
*/

locals {
  name_prefix   = "${var.team_name}-${var.environment}"
  bronze_bucket = "${local.name_prefix}-bronze"
  silver_bucket = "${local.name_prefix}-silver"
  gold_bucket   = "${local.name_prefix}-gold"
}

# ---------------------------------------------------------------------------
# S3 Buckets
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "bronze" {
  bucket        = local.bronze_bucket
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket" "silver" {
  bucket        = local.silver_bucket
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket" "gold" {
  bucket        = local.gold_bucket
  force_destroy = var.environment != "prod"
}

# Block public access on all buckets
resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# SSE-KMS encryption with bucket key (cost optimisation)
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

# Versioning on Silver and Gold (Bronze is immutable by convention)
resource "aws_s3_bucket_versioning" "silver" {
  count  = var.enable_versioning ? 1 : 0
  bucket = aws_s3_bucket.silver.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_versioning" "gold" {
  count  = var.enable_versioning ? 1 : 0
  bucket = aws_s3_bucket.gold.id
  versioning_configuration { status = "Enabled" }
}

# Lifecycle policies
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    id     = "archive-bronze"
    status = "Enabled"
    transition {
      days          = var.bronze_retention_days
      storage_class = "GLACIER_IR"
    }
    expiration { days = 1825 }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  count  = var.enable_versioning ? 1 : 0
  bucket = aws_s3_bucket.silver.id

  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    noncurrent_version_expiration { noncurrent_days = 30 }
  }

  rule {
    id     = "expire-silver"
    status = "Enabled"
    expiration { days = var.silver_retention_days }
  }
}

# ---------------------------------------------------------------------------
# Glue Data Catalog
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "bronze" {
  name        = "${replace(local.name_prefix, "-", "_")}_bronze"
  description = "Bronze (raw) layer for ${var.team_name} in ${var.environment}"
}

resource "aws_glue_catalog_database" "silver" {
  name        = "${replace(local.name_prefix, "-", "_")}_silver"
  description = "Silver (cleansed) layer for ${var.team_name} in ${var.environment}"
}

resource "aws_glue_catalog_database" "gold" {
  name        = "${replace(local.name_prefix, "-", "_")}_gold"
  description = "Gold (serving) layer for ${var.team_name} in ${var.environment}"
}

# ---------------------------------------------------------------------------
# IAM: Reader policy (SELECT access to Silver + Gold via Spectrum)
# ---------------------------------------------------------------------------

resource "aws_iam_policy" "reader" {
  name        = "${local.name_prefix}-datalake-reader"
  description = "Read access to ${var.team_name} Silver and Gold S3 buckets and Glue catalog"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadSilverGold"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*",
        ]
      },
      {
        Sid    = "GlueCatalogRead"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases",
          "glue:GetTable", "glue:GetTables",
          "glue:GetPartition", "glue:GetPartitions",
          "glue:GetUserDefinedFunction",
        ]
        Resource = [
          "arn:aws:glue:*:*:catalog",
          aws_glue_catalog_database.silver.arn,
          aws_glue_catalog_database.gold.arn,
          "${aws_glue_catalog_database.silver.arn}/*",
          "${aws_glue_catalog_database.gold.arn}/*",
        ]
      },
      {
        Sid      = "KMSDecrypt"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = [var.kms_key_arn]
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# IAM: Writer policy (write to all three layers)
# ---------------------------------------------------------------------------

resource "aws_iam_policy" "writer" {
  name        = "${local.name_prefix}-datalake-writer"
  description = "Write access to all ${var.team_name} data lake buckets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3WriteAll"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket", "s3:GetBucketLocation",
          "s3:GetObjectVersion", "s3:ListBucketMultipartUploads",
          "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
        ]
        Resource = [
          aws_s3_bucket.bronze.arn, "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn, "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,   "${aws_s3_bucket.gold.arn}/*",
        ]
      },
      {
        Sid    = "GlueCatalogWrite"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases",
          "glue:GetTable", "glue:GetTables",
          "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
          "glue:GetPartition", "glue:GetPartitions",
          "glue:CreatePartition", "glue:UpdatePartition", "glue:DeletePartition",
          "glue:BatchCreatePartition",
        ]
        Resource = ["*"]
      },
      {
        Sid      = "KMSEncryptDecrypt"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource = [var.kms_key_arn]
      }
    ]
  })
}

# Attach reader policy to provided role ARNs
resource "aws_iam_role_policy_attachment" "readers" {
  for_each   = toset(var.reader_role_arns)
  role       = each.value
  policy_arn = aws_iam_policy.reader.arn
}

resource "aws_iam_role_policy_attachment" "writers" {
  for_each   = toset(var.writer_role_arns)
  role       = each.value
  policy_arn = aws_iam_policy.writer.arn
}
