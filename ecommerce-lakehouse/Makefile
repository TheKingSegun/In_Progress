.PHONY: help local-up local-down spark-test dbt-run-local dbt-test \
        dbt-docs tf-init tf-plan tf-apply lint fmt

ENVIRONMENT ?= dev
RUN_DATE    ?= $(shell date +%Y-%m-%d)

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Local development
# ---------------------------------------------------------------------------

local-up: ## Start local stack (MinIO, Spark, Postgres, Airflow)
	docker compose up -d --wait
	@echo "Services ready:"
	@echo "  Airflow UI:   http://localhost:8090 (admin/admin)"
	@echo "  MinIO:        http://localhost:9001 (minio/minio123)"
	@echo "  Spark UI:     http://localhost:8080"

local-down: ## Stop and remove all local containers
	docker compose down -v

local-logs: ## Tail logs from all services
	docker compose logs -f

# ---------------------------------------------------------------------------
# Spark / PySpark
# ---------------------------------------------------------------------------

spark-test: ## Run PySpark unit tests
	cd spark && \
	pip install -r requirements.txt -q && \
	pytest tests/ -v --tb=short --cov=jobs --cov=utils --cov-report=term-missing

spark-submit-bronze: ## Run bronze ingestion locally (requires local-up)
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/jobs/jobs/bronze/ingest_orders.py \
		--run-date $(RUN_DATE) \
		--source-path s3://datalake/raw/orders \
		--bronze-path s3://datalake/bronze/orders \
		--quarantine-path s3://datalake/quarantine

# ---------------------------------------------------------------------------
# dbt
# ---------------------------------------------------------------------------

dbt-deps: ## Install dbt packages
	cd dbt && dbt deps

dbt-run-local: dbt-deps ## Run all dbt models against local Postgres
	cd dbt && dbt run --target local --profiles-dir .

dbt-test: ## Run all dbt tests
	cd dbt && dbt test --target local --profiles-dir .

dbt-build: dbt-deps ## Run + test all models
	cd dbt && dbt build --target local --profiles-dir .

dbt-docs: dbt-deps ## Generate and serve dbt docs
	cd dbt && dbt docs generate --target local --profiles-dir . && dbt docs serve

dbt-snapshot: ## Run snapshots (SCD Type 2)
	cd dbt && dbt snapshot --target local --profiles-dir .

# ---------------------------------------------------------------------------
# Terraform
# ---------------------------------------------------------------------------

tf-init: ## Initialise Terraform for the given environment
	cd terraform && terraform init -backend-config=environments/$(ENVIRONMENT).tfbackend

tf-plan: ## Plan Terraform changes
	cd terraform && terraform plan -var-file=environments/$(ENVIRONMENT).tfvars

tf-apply: ## Apply Terraform changes (requires confirmation)
	cd terraform && terraform apply -var-file=environments/$(ENVIRONMENT).tfvars

tf-fmt: ## Format Terraform files
	cd terraform && terraform fmt -recursive

tf-validate: ## Validate Terraform configuration
	cd terraform && terraform validate

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

lint: ## Run all linters
	ruff check spark/
	cd dbt && dbt parse --target local --profiles-dir .
	cd terraform && terraform validate

fmt: ## Auto-format Python and Terraform
	ruff format spark/
	cd terraform && terraform fmt -recursive
