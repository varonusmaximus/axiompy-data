.PHONY: help lint test coverage security

help:
	@echo "axiompy-data — local commands (mirror CI where possible)"
	@echo ""
	@echo "  make lint       - ruff check + ruff format --check"
	@echo "  make test       - pytest tests/"
	@echo "  make coverage   - pytest with coverage + fail-under=80"
	@echo "  make security   - bandit + pip-audit"
	@echo ""
	@echo "Optional: pip install pre-commit && pre-commit install --hook-type pre-push"
	@echo "          pre-commit run --all-files"

lint:
	ruff check . --config pyproject.toml
	ruff format --check . --config pyproject.toml

test:
	pytest tests/ -v --tb=short

coverage:
	pytest tests/ --cov=axiompy --cov-report=term
	coverage report --fail-under=80

security:
	bandit -r axiompy/ -ll
	pip-audit
