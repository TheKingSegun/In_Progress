# E-Commerce Lakehouse Platform

## Business Context

A mid-to-large e-commerce company processes **5M+ orders per day** across web, mobile, and marketplace channels. The data team needed to replace a fragile, monolithic ETL system (nightly SQL Server jobs, no lineage, no tests) with a scalable, observable, cloud-native analytics platform.

**Outcomes delivered:**
- Reduced data freshness from T+24h to T+1h for operational reporting
- Enabled self-serve analytics for 80+ business users via Redshift + BI tooling
- Cut infrastructure costs 42% by moving from always-on clusters to serverless/spot-based processing
- Achieved full data lineage and column-level impact analysis via dbt docs

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Data Sources                               │
│  Transactional DB  │  Kafka Events  │  3rd Party APIs  │  Files │
└────────────┬────────────────┬────────────────┬──────────────────┘
             │                │                │
             ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw)                           │
│  S3://datalake/bronze/   ·   Schema-on-read   ·   Parquet/JSON  │
│  PySpark ingestion jobs on EMR  ·  Schema registry              │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Cleansed)                      │
│  S3://datalake/silver/   ·   Validated   ·   Delta Parquet      │
│  PySpark transform jobs  ·  Great Expectations quality checks   │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER (Serving)                        │
│  Amazon Redshift  ·  dbt models  ·  Dimensional + OBT models   │
│  Staging → Intermediate → Marts  ·  Automated tests & docs     │
└─────────────────────────────────────────────────────────────────┘
             │                │                │
             ▼                ▼                ▼
      BI Tools          Data Science        Operational
    (Tableau/Metabase)   (Notebooks)         APIs
```

**Orchestration:** Apache Airflow (MWAA) coordinates all layers with SLA monitoring and Slack alerting.

---

## Project Structure

```
ecommerce-lakehouse/
├── terraform/              # Infrastructure as Code (AWS)
│   └── modules/
│       ├── s3_data_lake/   # S3 buckets, lifecycle policies, encryption
│       ├── glue_catalog/   # Glue data catalog and crawlers
│       ├── redshift_cluster/ # Redshift provisioned cluster
│       └── mwaa/           # Managed Airflow environment
├── spark/
│   ├── jobs/
│   │   ├── bronze/         # Raw ingestion (CDC, API pulls, file landing)
│   │   ├── silver/         # Cleaning, validation, deduplication
│   │   └── gold/           # Business aggregations and metric computation
│   ├── utils/              # Shared Spark session, logging, quality helpers
│   └── tests/              # PySpark unit tests with pytest + chispa
├── dbt/
│   ├── models/
│   │   ├── staging/        # 1:1 source models, type casting, snake_case
│   │   ├── intermediate/   # Business logic joins, deduplication
│   │   └── marts/
│   │       ├── core/       # dim_customers, dim_products, fct_orders
│   │       └── finance/    # Revenue, refunds, margin models
│   ├── macros/             # Reusable SQL macros
│   ├── snapshots/          # SCD Type 2 for slowly-changing dimensions
│   └── tests/              # Custom generic and singular tests
├── airflow/
│   ├── dags/               # Pipeline DAGs with sensors and callbacks
│   └── plugins/            # Custom operators (EMR, dbt cloud API)
└── .github/workflows/      # CI: lint, test, dbt slim build, tf plan
```

---

## Key Engineering Decisions

### Why Medallion Architecture?
Each layer has a clear contract. Bronze is immutable raw data — if a bug is found in Silver, we replay from Bronze without re-pulling source systems. This makes debugging production incidents significantly cheaper.

### Why dbt on Redshift instead of pure Spark Gold?
Business analysts can write SQL and contribute models. dbt's test framework, docs site, and lineage graph give non-engineers visibility into what data means and where it comes from. Spark gold jobs are reserved for heavy pre-aggregations that don't belong in a warehouse.

### Why MWAA over self-managed Airflow?
At this scale, the operational overhead of managing Airflow on EC2/ECS outweighs the cost premium of MWAA. The team stays focused on pipelines, not platform maintenance.

### SCD Type 2 on Customers
Customer attributes (name, address, tier) change over time. Snapshots ensure we can answer "what segment was this customer in when they placed this order?" — critical for cohort analysis and A/B testing attribution.

---

## Running Locally

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- AWS CLI configured (for Terraform)
- Terraform 1.6+

### Start local stack
```bash
make local-up          # Starts Spark, Airflow, Postgres (Redshift proxy), MinIO (S3 proxy)
make spark-test        # Run PySpark unit tests
make dbt-run-local     # Run dbt against local Postgres
make dbt-test          # Run dbt tests
make local-down        # Tear down
```

### Deploy infrastructure
```bash
cd terraform
terraform init -backend-config=environments/dev.tfbackend
terraform plan -var-file=environments/dev.tfvars
terraform apply -var-file=environments/dev.tfvars
```

---

## Data Quality Strategy

| Layer | Tool | Checks |
|-------|------|--------|
| Bronze | PySpark + Great Expectations | Schema conformance, null rate thresholds, volume anomalies |
| Silver | PySpark custom validators | Business rule validation, referential integrity, dedup |
| Gold (dbt) | dbt tests | not_null, unique, accepted_values, relationships, custom SQL |

Any quality failure blocks downstream layers and triggers a PagerDuty alert.

---

## CI/CD Pipeline

```
PR opened
  └─► Python lint (ruff) + type check (mypy)
  └─► PySpark unit tests (pytest + chispa)
  └─► dbt slim build (only changed models + downstream)
  └─► dbt test (changed models only)
  └─► Terraform plan (no apply)
  └─► Security scan (checkov on Terraform)

Merge to main
  └─► Full dbt build in staging
  └─► Terraform apply to staging
  └─► Smoke tests against staging Redshift
  └─► Manual gate → production deploy
```
