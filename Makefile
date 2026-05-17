.PHONY: help venv lint test coverage security typecheck ci-local precommit-install

# Prefer an activated venv or ./.venv when present
VENV_DETECT := $(shell \
	if [ -n "$$VIRTUAL_ENV" ]; then basename "$$VIRTUAL_ENV"; \
	elif [ -d ".venv" ] && [ -f ".venv/bin/activate" ]; then echo ".venv"; \
	else echo ""; fi)
PYTHON := $(if $(VENV_DETECT),./$(VENV_DETECT)/bin/python,python3.12)

help:
	@echo "axiompy-data — local commands (mirror CI where possible)"
	@echo ""
	@echo "  make venv              - create .venv with Python 3.12+"
	@echo "  make lint              - ruff check + ruff format --check"
	@echo "  make test              - pytest tests/"
	@echo "  make coverage          - pytest with coverage + fail-under=80"
	@echo "  make security          - bandit + pip-audit"
	@echo "  make typecheck         - mypy axiompy/ (optional; not in CI)"
	@echo "  make ci-local          - lint + pre-commit + coverage + security"
	@echo "  make precommit-install - install pre-commit git hooks"
	@echo ""
	@echo "Install: pip install -e \"../axiompy[io]\" && pip install -e \".[dev]\" (or [test-all])"

venv:
	@PY=$$(command -v python3.12 || command -v python3); \
	$$PY -m venv .venv; \
	./.venv/bin/pip install --upgrade pip; \
	./.venv/bin/pip install -e "../axiompy[io]" 2>/dev/null || true; \
	./.venv/bin/pip install -e ".[dev]"; \
	$(MAKE) precommit-install
	@echo "Activate: source .venv/bin/activate"

lint:
	$(PYTHON) -m ruff check . --config pyproject.toml
	$(PYTHON) -m ruff format --check . --config pyproject.toml

test:
	$(PYTHON) -m pytest tests/ -v --tb=short

coverage:
	$(PYTHON) -m pytest tests/ --cov=axiompy --cov-report=term
	$(PYTHON) -m coverage report --fail-under=80

security:
	$(PYTHON) -m bandit -c pyproject.toml -r axiompy/ -ll
	$(PYTHON) -m pip_audit

typecheck:
	$(PYTHON) -m mypy -p axiompy.data --config-file pyproject.toml

ci-local: lint
	@if [ -z "$(VENV_DETECT)" ]; then \
		echo "Tip: create .venv (make venv) so pre-commit is available"; \
	elif [ -x "./$(VENV_DETECT)/bin/pre-commit" ]; then \
		./$(VENV_DETECT)/bin/pre-commit run --all-files; \
	else \
		echo "pre-commit not in venv — skipping hook run"; \
	fi
	$(MAKE) coverage
	$(MAKE) security

precommit-install:
	@if [ ! -d ".git" ]; then exit 0; fi; \
	if [ -z "$(VENV_DETECT)" ] || [ ! -x "./$(VENV_DETECT)/bin/pre-commit" ]; then \
		echo "pre-commit not installed — pip install -e \".[dev]\""; exit 0; \
	fi; \
	./$(VENV_DETECT)/bin/pre-commit install; \
	./$(VENV_DETECT)/bin/pre-commit install --hook-type pre-push; \
	echo "Git hooks installed"
