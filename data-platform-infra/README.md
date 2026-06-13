# Data Platform Infrastructure (Terraform)

## Business Context

A growing data team needed to go from a single-account AWS environment with manually-created resources to a **multi-environment, multi-account, fully automated infrastructure** that supports five engineering teams building data products independently — without stepping on each other's toes.

**Goals:**
- Zero manual AWS console clicks for any data infrastructure
- Consistent environments: dev mirrors staging mirrors prod
- Self-service data product provisioning (new S3 + Glue + IAM in <10 minutes)
- Full audit trail — every infrastructure change goes through code review and Terraform plan approval
- Cost visibility: per-environment, per-team tagging enforced by OPA policies

**Outcomes delivered:**
- Reduced environment provisioning time from 3 weeks (manual) to 45 minutes (Terraform)
- Infrastructure drift incidents dropped to zero
- Onboarded 3 new data product teams in 2 weeks with self-service modules
- Monthly AWS spend visibility improved: 100% of resources have team/environment tags

---

## Module Catalogue

```
data-platform-infra/
├── modules/
│   ├── data-lake/           # S3 buckets, lifecycle, encryption, Glue catalog
│   ├── data-warehouse/      # Redshift cluster + parameter groups + IAM
│   ├── orchestration/       # MWAA (Airflow) + ECR for custom images
│   ├── streaming/           # MSK (Kafka) + Schema Registry
│   └── networking/          # VPC, subnets, security groups, VPC endpoints
├── environments/
│   ├── dev/                 # Lightweight, cost-optimised development environment
│   ├── staging/             # Production-mirrored staging environment
│   └── prod/                # Production environment with HA and multi-AZ
└── .github/workflows/
    ├── terraform-plan.yml   # Plan on every PR
    └── terraform-apply.yml  # Apply on merge to main (with environment gates)
```

---

## Module Usage

### Provision a new data lake for a domain team

```hcl
module "payments_data_lake" {
  source = "../../modules/data-lake"

  team_name    = "payments"
  environment  = var.environment
  kms_key_arn  = module.kms.key_arn

  enable_versioning     = true
  bronze_retention_days = 90
  silver_retention_days = 365
  gold_retention_days   = 730

  # IAM principals that need read access (e.g. Redshift spectrum, EMR)
  reader_role_arns = [
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/emr-instance-profile",
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/redshift-spectrum-role",
  ]

  # IAM principals that can write (e.g. Spark job role, Airflow)
  writer_role_arns = [
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/mwaa-execution-role",
  ]
}
```

### Provision a Redshift cluster

```hcl
module "analytics_warehouse" {
  source = "../../modules/data-warehouse"

  environment         = var.environment
  cluster_identifier  = "analytics-${var.environment}"
  node_type           = var.environment == "prod" ? "ra3.4xlarge" : "ra3.xlplus"
  number_of_nodes     = var.environment == "prod" ? 3 : 1
  database_name       = "analytics"
  kms_key_arn         = module.kms.key_arn
  vpc_id              = module.networking.vpc_id
  subnet_ids          = module.networking.private_subnet_ids
  s3_data_lake_arns   = [module.payments_data_lake.silver_bucket_arn]
}
```

---

## Environment Strategy

| Feature | Dev | Staging | Prod |
|---------|-----|---------|------|
| Redshift node type | `ra3.xlplus` | `ra3.xlplus` | `ra3.4xlarge` |
| Redshift nodes | 1 | 1 | 3 |
| MSK broker type | `kafka.t3.small` | `kafka.m5.large` | `kafka.m5.large` |
| MSK brokers | 1 | 3 | 3 |
| MWAA class | `mw1.small` | `mw1.medium` | `mw1.large` |
| Multi-AZ | No | Yes | Yes |
| Deletion protection | Off | On | On |
| Backup retention | 1 day | 7 days | 35 days |
| S3 versioning | Off | On | On |

---

## CI/CD Workflow

```
PR opened
  └─► Terraform fmt check
  └─► Terraform validate
  └─► tflint (module-level lint rules)
  └─► Checkov security scan
  └─► Terraform plan (dev) → posted as PR comment
  └─► Infracost estimate → cost delta posted as PR comment

Merge to main
  └─► Terraform apply → dev (auto)
  └─► Run integration smoke tests against dev
  └─► Manual approval gate (Slack) → staging apply
  └─► Manual approval gate (Slack + second engineer) → prod apply
```

---

## Security Controls

- **All S3 buckets**: Block public access, SSE-KMS, access logging
- **All databases**: Encryption at rest (KMS), VPC-only access, no public endpoints
- **IAM**: Least privilege — each role has only the S3 paths and actions it needs
- **KMS**: Per-environment KMS keys with key rotation enabled
- **VPC**: All data services are in private subnets; internet access via NAT Gateway only
- **Secrets**: No credentials in Terraform — Redshift passwords via AWS Secrets Manager reference; MSK uses SASL/SCRAM via Secrets Manager
- **Checkov**: All PRs blocked on HIGH/CRITICAL security findings

---

## Local Usage

```bash
# Validate all modules
make validate-all

# Plan for dev
make plan ENV=dev

# Apply to dev (CI does this automatically on merge)
make apply ENV=dev

# Estimate cost impact of a change
make cost-estimate ENV=prod
```
