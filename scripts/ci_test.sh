#!/usr/bin/env bash
# =============================================================================
# Full test suite of poc_dbt on DuckDB, with test data: no warehouse, no cost,
# no credentials. Used by both Cloud Build pipelines (cloudbuild-pr.yaml and
# cloudbuild.yaml) and runnable locally (Linux, macOS, Git Bash):
#
#   pip install -r requirements-dev.txt
#   bash scripts/ci_test.sh
#
# Steps:
#   1. Style: SQLFluff on the SQL, pytest on the Airflow DAG helpers.
#   2. Rules: naming convention and documentation rules (run-operations).
#   3. Data: test landing tables, then a full dbt build (unit tests, models,
#      data tests, snapshots, seeds). The incremental model is first created
#      empty, because its unit test reads the existing table ({{ this }}).
#   4. Second pass: incremental run and snapshot changes on existing tables.
# =============================================================================
set -euo pipefail

export DBT_PROFILES_DIR=.
export DBT_ADAPTER=duckdb
export DBT_TARGET=ci
export DBT_USER=ci
export DBT_TARGET_PATH="${DBT_TARGET_PATH:-target_ci}"
export DBT_DUCKDB_PATH="${DBT_DUCKDB_PATH:-target_ci/ci.duckdb}"

mkdir -p target_ci
rm -f "${DBT_DUCKDB_PATH}" "${DBT_DUCKDB_PATH}.wal"   # every run starts from an empty database

echo "== 1. Style"
sqlfluff lint models analyses tests
pytest -q -p no:cacheprovider dags/tests

echo "== 2. Rules"
dbt deps
dbt run-operation validate_model_naming
dbt run-operation validate_model_documentation

echo "== 3. Data"
dbt run-operation load_ci_landing_data
dbt run --empty --select +config.materialized:incremental
dbt build

echo "== 4. Second pass"
dbt build --select config.materialized:incremental resource_type:snapshot

echo "== All checks passed"
