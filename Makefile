VENV_DIR ?= .venv
VENV_PYTHON := $(VENV_DIR)/bin/python
VENV_PIP := $(VENV_DIR)/bin/pip
PYTHON ?= $(shell if [ -x "$(VENV_PYTHON)" ]; then echo "$(VENV_PYTHON)"; else echo python3; fi)
INPUT_PARQUET ?=
YEAR ?= 2024
MONTH ?= 1
OUTPUT_DIR ?= data/raw
MAX_INVALID_RATIO ?= 0.001
WAREHOUSE_DIR ?=
STRICT_QUALITY ?= 0

COMMON_ARGS = $(if $(YEAR),--year $(YEAR),) $(if $(MONTH),--month $(MONTH),) --max-invalid-ratio $(MAX_INVALID_RATIO) $(if $(WAREHOUSE_DIR),--warehouse-dir $(WAREHOUSE_DIR),)
STRICT_QUALITY_ARG = $(if $(filter 1 true TRUE yes YES,$(STRICT_QUALITY)),--strict-quality,)
INPUT_ARG = $(if $(INPUT_PARQUET),--input-parquet "$(INPUT_PARQUET)",)

.PHONY: setup venv lint fmt fmt-check test check contracts smoke run-local local-smoke download inspect bronze silver gold quality run-all run full-run clean reset

setup:
	@echo "[setup] ensuring virtual environment at $(VENV_DIR)"
	@if [ ! -x "$(VENV_PYTHON)" ]; then \
		python3 -m venv "$(VENV_DIR)"; \
		echo "[setup] created $(VENV_DIR)"; \
	else \
		echo "[setup] $(VENV_DIR) already exists"; \
	fi
	@echo "[setup] installing dependencies from requirements.txt"
	@"$(VENV_PIP)" install --upgrade pip
	@"$(VENV_PIP)" install -r requirements.txt

venv:
	@$(MAKE) setup

lint:
	@echo "[lint] running ruff check"
	$(PYTHON) -m ruff check .

fmt:
	@echo "[fmt] running ruff format"
	$(PYTHON) -m ruff format .

fmt-check:
	@echo "[fmt-check] running ruff format --check"
	$(PYTHON) -m ruff format --check .

test:
	@echo "[test] running pytest -q"
	$(PYTHON) -m pytest -q

check: fmt-check lint test

contracts:
	@echo "[contracts] validating data contracts"
	$(PYTHON) ci/scripts/validate_contracts.py
	@echo "[contracts] detecting breaking changes"
	$(PYTHON) ci/scripts/detect_contract_breaking_changes.py

smoke:
	@echo "[smoke] running import smoke checks"
	$(PYTHON) ci/scripts/smoke_imports.py

run-local: local-smoke

local-smoke:
	@echo "[local-smoke] running local smoke flow for YEAR=$(YEAR) MONTH=$(MONTH)"
	YEAR=$(YEAR) MONTH=$(MONTH) MAX_INVALID_RATIO=$(MAX_INVALID_RATIO) WAREHOUSE_DIR="$(WAREHOUSE_DIR)" INPUT_PARQUET="$(INPUT_PARQUET)" ./scripts/local_smoke_test.sh

download:
	@echo "[download] fetching TLC parquet for YEAR=$(YEAR) MONTH=$(MONTH)"
	$(PYTHON) orchestration/local/run_pipeline.py download --year $(YEAR) --month $(MONTH) --output-dir "$(OUTPUT_DIR)"

inspect:
	@echo "[inspect] listing local metastore objects"
	$(PYTHON) orchestration/local/run_pipeline.py inspect $(if $(WAREHOUSE_DIR),--warehouse-dir "$(WAREHOUSE_DIR)",)

bronze:
	@echo "[bronze] running Bronze stage"
	$(PYTHON) orchestration/local/run_pipeline.py run-bronze $(INPUT_ARG) $(COMMON_ARGS)

silver:
	@echo "[silver] running Silver stage"
	$(PYTHON) orchestration/local/run_pipeline.py run-silver $(COMMON_ARGS)

gold:
	@echo "[gold] running Gold stage"
	$(PYTHON) orchestration/local/run_pipeline.py run-gold $(COMMON_ARGS)

quality:
	@echo "[quality] running quality stage (strict=$(STRICT_QUALITY))"
	$(PYTHON) orchestration/local/run_pipeline.py run-quality $(COMMON_ARGS) $(STRICT_QUALITY_ARG)

run-all:
	@echo "[run-all] running full pipeline for YEAR=$(YEAR) MONTH=$(MONTH)"
	$(PYTHON) orchestration/local/run_pipeline.py run-all $(INPUT_ARG) $(COMMON_ARGS) $(STRICT_QUALITY_ARG)

run: run-all

full-run: run-all

clean:
	@echo "[clean] dropping quality tables"
	$(PYTHON) orchestration/local/run_pipeline.py clean $(COMMON_ARGS) --remove-quality-warehouse

reset:
	@echo "[reset] dropping managed tables"
	$(PYTHON) orchestration/local/run_pipeline.py reset $(COMMON_ARGS)
