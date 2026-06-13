# Real-Time Fraud Detection Pipeline

## Business Context

A fintech company processes **2M+ card transactions per day** across B2C and B2B products. Fraud losses were running at 0.8% of GMV — significantly above the industry benchmark of 0.1–0.2%. The existing batch fraud model (nightly run, next-day review) was too slow to prevent losses on a fast-moving threat landscape.

**Solution:** A real-time streaming pipeline that scores every transaction within **200ms** of authorisation, enabling the payments gateway to block suspicious transactions before they complete.

**Outcomes delivered:**
- Reduced fraud loss rate from 0.8% to 0.18% within 60 days of production launch
- Zero increase in false positive rate (legitimate transaction decline rate held at <0.5%)
- Saved ~$4.2M in annualised fraud losses at $800M GMV
- Enabled the risk team to tune rules without engineering involvement via a feature flag service

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         Transaction Events                               │
│   Payments Gateway  ──►  Kafka Topic: transactions.raw  (MSK)           │
└──────────────────────────────────┬───────────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │   PySpark Structured         │
                    │   Streaming (EMR Streaming)  │
                    │                              │
                    │  1. Enrich with features     │◄── Redis (feature store)
                    │  2. Score with rules engine  │
                    │  3. Emit decision            │
                    └──────┬──────────────┬────────┘
                           │              │
               ┌───────────▼──┐    ┌──────▼──────────────┐
               │ Kafka Topic  │    │ Kafka Topic          │
               │ fraud.alerts │    │ transactions.scored  │
               └───────┬──────┘    └──────┬───────────────┘
                       │                  │
              ┌────────▼──────┐    ┌──────▼───────────┐
              │ Alert Service │    │ Postgres (audit   │
              │ (PagerDuty /  │    │ trail + offline   │
              │  SMS)         │    │ model training)   │
              └───────────────┘    └──────────────────-┘
                                          │
                              ┌───────────▼──────────────┐
                              │  dbt batch pipeline       │
                              │  (nightly feature         │
                              │   recomputation for ML)   │
                              └───────────────────────────┘
```

**Feature Store Architecture:**
- **Online** (Redis): Pre-computed features served at <5ms latency. Updated by the streaming job itself and by a nightly batch refresh.
- **Offline** (Postgres + dbt): Full historical feature table for model training and backtesting.

---

## Project Structure

```
realtime-fraud-detection/
├── terraform/
│   └── modules/
│       ├── msk/          # Amazon MSK (Kafka) cluster
│       ├── emr/          # EMR Streaming cluster
│       └── elasticache/  # Redis for online feature store
├── streaming/
│   ├── producer/         # Transaction event simulator (for dev/testing)
│   │   └── transaction_producer.py
│   ├── processor/        # PySpark Streaming fraud detection job
│   │   ├── fraud_detection_stream.py
│   │   └── rules_engine.py
│   └── feature_store/
│       ├── feature_definitions.py   # Feature computation logic
│       └── redis_client.py          # Online feature store client
├── dbt/
│   ├── models/
│   │   ├── staging/      # Raw transaction sources
│   │   └── marts/        # Offline feature tables for ML training
│   └── tests/
└── .github/workflows/
    └── ci.yml
```

---

## Fraud Detection Logic

### Rules Engine (Tier 1 — Deterministic)
Fast, explainable rules that catch known fraud patterns:

| Rule | Signal | Action |
|------|--------|--------|
| Velocity check | >5 transactions in 1 minute from same card | Block + Alert |
| Amount spike | Transaction > 3× the card's 30-day average | Hold for review |
| Geo-impossibility | Transaction location >500km from previous in <1h | Block |
| New device + high amount | First transaction on device >$500 | Hold |
| Blacklisted merchant | Merchant ID in fraud watchlist | Block |

### ML Scoring (Tier 2 — Probabilistic)
An XGBoost model (retrained weekly on offline features) produces a fraud probability score 0–1. Transactions with score >0.85 are blocked; 0.6–0.85 go to human review.

The ML model is out of scope for this repo — this pipeline provides the **feature enrichment infrastructure** the model consumes.

---

## Running Locally

```bash
# Start Kafka, Redis, Postgres, and Spark
docker compose up -d

# Start the transaction event simulator
python streaming/producer/transaction_producer.py --rate 100  # 100 tx/sec

# Submit the Spark Streaming job
docker compose exec spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  streaming/processor/fraud_detection_stream.py \
  --kafka-bootstrap-servers kafka:9092 \
  --redis-host redis

# Monitor the fraud alerts topic
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic fraud.alerts \
  --from-beginning
```

---

## Infrastructure

```bash
cd terraform
terraform init -backend-config=environments/dev.tfbackend
terraform plan -var-file=environments/dev.tfvars
terraform apply -var-file=environments/dev.tfvars
```

Terraform provisions:
- **MSK**: 3-broker Kafka cluster, `kafka.m5.large`, multi-AZ
- **EMR Streaming**: Auto-scaling cluster (2–10 workers) based on Kafka consumer lag
- **ElastiCache Redis**: `cache.r6g.large`, cluster mode enabled, encrypted at rest and in transit

---

## Key Engineering Decisions

### Why PySpark Streaming instead of Flink?
The team already had Spark expertise from the batch lakehouse. PySpark Structured Streaming's exactly-once semantics and native Kafka integration met our latency requirements (<200ms end-to-end). Flink would add operational complexity without a compelling benefit at our scale.

### Why Redis for the online feature store instead of DynamoDB?
Sub-millisecond read latency is critical when the payment gateway is waiting. Redis with in-memory data gives ~0.3ms reads vs ~5ms for DynamoDB. We accept the operational complexity of Redis cluster management in exchange for the latency guarantee.

### Why a rules engine AND a model?
Rules are fast to deploy, fully explainable to regulators, and catch known patterns with 100% precision. The ML model catches novel patterns the rules miss. Together they achieve better recall than either alone, and the explainability of rules satisfies PCI-DSS audit requirements.
