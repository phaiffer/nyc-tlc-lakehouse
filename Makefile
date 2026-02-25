PYTHON ?= ./.venv/bin/python
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

.PHONY: venv lint contracts smoke download inspect bronze silver gold quality run-all full-run clean reset

venv:
	python3 -m venv .venv
	./.venv/bin/pip install --upgrade pip
	./.venv/bin/pip install -r requirements.txt

lint:
	$(PYTHON) -m ruff check .

contracts:
	$(PYTHON) ci/scripts/validate_contracts.py
	$(PYTHON) ci/scripts/detect_contract_breaking_changes.py

smoke:
	$(PYTHON) -m compileall .
	$(PYTHON) ci/scripts/smoke_imports.py

download:
	$(PYTHON) orchestration/local/run_pipeline.py download --year $(YEAR) --month $(MONTH) --output-dir "$(OUTPUT_DIR)"

inspect:
	$(PYTHON) orchestration/local/run_pipeline.py inspect $(if $(WAREHOUSE_DIR),--warehouse-dir "$(WAREHOUSE_DIR)",)

bronze:
	$(PYTHON) orchestration/local/run_pipeline.py run-bronze $(INPUT_ARG) $(COMMON_ARGS)

silver:
	$(PYTHON) orchestration/local/run_pipeline.py run-silver $(COMMON_ARGS)

gold:
	$(PYTHON) orchestration/local/run_pipeline.py run-gold $(COMMON_ARGS)

quality:
	$(PYTHON) orchestration/local/run_pipeline.py run-quality $(COMMON_ARGS) $(STRICT_QUALITY_ARG)

run-all:
	$(PYTHON) orchestration/local/run_pipeline.py run-all $(INPUT_ARG) $(COMMON_ARGS) $(STRICT_QUALITY_ARG)

full-run: run-all

clean:
	$(PYTHON) orchestration/local/run_pipeline.py clean $(COMMON_ARGS) --remove-quality-warehouse

reset:
	$(PYTHON) orchestration/local/run_pipeline.py reset $(COMMON_ARGS)
