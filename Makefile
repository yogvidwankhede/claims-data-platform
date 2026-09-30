PY      ?= .venv/bin/python
BIN      = .venv/bin
AIRFLOW_PY ?= .venv-airflow/bin/python
AIRFLOW_VERSION = 3.3.2
PYVER   := $(shell python3 -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')

.PHONY: install install-airflow lint test test-unit test-delta test-airflow dbt e2e clean

install:            ## platform + Spark/Delta + dbt + dev tools
	python3 -m venv .venv && $(BIN)/pip install -q -e ".[dev]"

install-airflow:    ## Airflow in its own venv: it orchestrates, it never imports the platform
	python3 -m venv .venv-airflow && .venv-airflow/bin/pip install -q "apache-airflow==$(AIRFLOW_VERSION)" pytest \
	  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYVER).txt"

lint:
	$(BIN)/ruff check src tests airflow && $(BIN)/ruff format --check src tests airflow
	$(BIN)/sqlfluff lint snowflake/migrations --config snowflake/.sqlfluff
	cd dbt && ../$(BIN)/dbt parse --profiles-dir . --quiet

test-unit:          ## fast: transforms (plain Spark), contracts, Snowflake, bundle, DQ
	$(PY) -m pytest -q -m "not delta"

test-delta:         ## Delta I/O integration (needs the Delta jars: Maven, or DELTA_JARS_DIR)
	$(PY) -m pytest -q -m delta

test-airflow:
	AIRFLOW_HOME=$$(mktemp -d) $(AIRFLOW_PY) -m pytest -q airflow/tests

test: test-unit test-delta test-airflow

dbt:
	cd dbt && ../$(BIN)/dbt build --profiles-dir .

e2e:                ## 5 days through the whole platform, as the DAG runs them
	PATH=$(CURDIR)/$(BIN):$$PATH scripts/e2e.sh 2026-01-01 5

clean:
	rm -rf data dbt/target dbt/logs spark-warehouse metastore_db
