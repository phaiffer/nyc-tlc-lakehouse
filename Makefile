PYTHON ?= ./.venv/bin/python
INPUT_PARQUET ?=
YEAR ?=
MONTH ?=
MAX_INVALID_RATIO ?= 0.001
WAREHOUSE_DIR ?=
STRICT_QUALITY ?= 0

COMMON_ARGS = $(if $(YEAR),--year $(YEAR),) $(if $(MONTH),--month $(MONTH),) --max-invalid-ratio $(MAX_INVALID_RATIO) $(if $(WAREHOUSE_DIR),--warehouse-dir $(WAREHOUSE_DIR),)
STRICT_QUALITY_ARG = $(if $(filter 1 true TRUE yes YES,$(STRICT_QUALITY)),--strict-quality,)

.PHONY: venv lint contracts smoke bronze silver gold quality run-all full-run clean reset

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

bronze:
	@test -n "$(INPUT_PARQUET)" || (echo "INPUT_PARQUET is required" && exit 1)
	$(PYTHON) orchestration/local/run_pipeline.py run-bronze --input-parquet "$(INPUT_PARQUET)" $(COMMON_ARGS)

silver:
	$(PYTHON) orchestration/local/run_pipeline.py run-silver $(COMMON_ARGS)

gold:
	$(PYTHON) orchestration/local/run_pipeline.py run-gold $(COMMON_ARGS)

quality:
	$(PYTHON) orchestration/local/run_pipeline.py run-quality $(COMMON_ARGS) $(STRICT_QUALITY_ARG)

run-all:
	@test -n "$(INPUT_PARQUET)" || (echo "INPUT_PARQUET is required" && exit 1)
	$(PYTHON) orchestration/local/run_pipeline.py run-all --input-parquet "$(INPUT_PARQUET)" $(COMMON_ARGS) $(STRICT_QUALITY_ARG)

full-run: run-all

clean:
	$(PYTHON) orchestration/local/run_pipeline.py clean $(COMMON_ARGS) --remove-quality-warehouse

reset:
	$(PYTHON) orchestration/local/run_pipeline.py reset $(COMMON_ARGS)
