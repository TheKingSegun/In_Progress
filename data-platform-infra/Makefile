.PHONY: help validate-all fmt-all plan apply cost-estimate

ENV ?= dev

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

validate-all: ## Validate all Terraform modules and environments
	@for dir in modules/*/; do \
		echo ">>> Validating $$dir"; \
		cd $$dir && terraform init -backend=false -input=false > /dev/null && terraform validate && cd -; \
	done
	@echo "All modules valid"

fmt-all: ## Format all Terraform files in place
	terraform fmt -recursive

lint: ## Run tflint across all modules
	@for dir in modules/*/; do \
		echo ">>> Linting $$dir"; \
		tflint --chdir=$$dir; \
	done

security-scan: ## Run Checkov security analysis
	checkov -d . --framework terraform

plan: ## Plan Terraform for the given environment (make plan ENV=dev)
	cd environments/$(ENV) && terraform init && terraform plan -var-file=terraform.tfvars

apply: ## Apply Terraform for the given environment (make apply ENV=dev)
	cd environments/$(ENV) && terraform init && terraform apply -var-file=terraform.tfvars

destroy: ## Destroy environment (DANGEROUS — only use in dev)
	@if [ "$(ENV)" = "prod" ]; then echo "ERROR: Cannot destroy prod"; exit 1; fi
	cd environments/$(ENV) && terraform destroy -var-file=terraform.tfvars

cost-estimate: ## Run Infracost estimate for the given environment
	cd environments/$(ENV) && terraform plan -out=tfplan -var-file=terraform.tfvars && \
	infracost breakdown --path=tfplan
