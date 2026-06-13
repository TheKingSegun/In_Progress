# Senior Data Engineering Portfolio

A collection of production-grade data engineering projects demonstrating end-to-end platform design, implementation, and operations across the modern data stack.

## Projects

| Project | Business Problem | Stack |
|---------|-----------------|-------|
| [ecommerce-lakehouse-platform](./ecommerce-lakehouse/) | Scalable analytics platform for a high-volume e-commerce business | PySpark · dbt · Airflow · AWS · Terraform · Docker |
| [realtime-fraud-detection](./realtime-fraud-detection/) | Sub-second transaction fraud scoring at scale | Kafka · PySpark Streaming · Redis · Terraform · Docker |
| [data-platform-infra](./data-platform-infra/) | Reusable, multi-environment cloud data platform infrastructure | Terraform · AWS · GitHub Actions |

## Engineering Philosophy

- **Medallion architecture** (Bronze/Silver/Gold) for separation of concerns and incremental refinement
- **Infrastructure as Code** — every environment is reproducible and version-controlled
- **Data quality as a first-class concern** — tests at ingestion, transformation, and serving layers
- **Cost-aware design** — partitioning strategies, lifecycle policies, and cluster right-sizing built-in
- **CI/CD for data** — automated testing on PRs, slim dbt builds, Terraform plan gates

---

> These projects reflect real-world patterns used in enterprise data platforms handling billions of events per day.
