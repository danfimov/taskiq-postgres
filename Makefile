args := $(wordlist 2, 100, $(MAKECMDGOALS))

VIRTUAL_ENV=.venv

.DEFAULT:
	@echo "No such command (or you pass two or many targets to ). List of possible commands: make help"

.DEFAULT_GOAL := help

##@ Local development

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target> <arg=value>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m  %s\033[0m\n\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: venv
venv: ## Create a new virtual environment
	@uv venv

.PHONY: check_venv
check_venv: ## Check that virtual environment is activated
	@if [ -z $$VIRTUAL_ENV ]; then \
  		echo "Error: Virtual environment is not activated"; \
  		exit 1; \
	fi

.PHONY: init
init: ## Install all project dependencies with extras
	@$(MAKE) check_venv
	@uv sync --all-extras

.PHONY: run_docs
run_docs: ## Run documentation server
	@uv run mkdocs serve --livereload

.PHONY: run_infra
run_infra: ## Run rabbitmq in docker for integration tests
	@docker compose -f docker-compose.yml up -d

##@ Code quality

.PHONY: lint
lint: ## Run linting
	@$(MAKE) ruff
	@$(MAKE) mypy

.PHONY: ruff
ruff: ## Run ruff linting
	@uv run --active --frozen ruff check src tests

.PHONY: mypy
mypy: ## Run mypy type checking
	@uv run --active --frozen mypy src

.PHONY: format
format: ## Run formatting
	@uv run ruff check . --fix

##@ Testing

.PHONY: test
test: ## Run all pytest tests
	@uv run pytest tests

.PHONY: test_cov
test_cov:  ## Generate test coverage report
	@pytest --cov='src' --cov-report=html


.PHONY: test_install
test_install: ## Verify package installation by importing it
	@uv run --with taskiq-postgres --no-project -- python -c "import taskiq_pg"
