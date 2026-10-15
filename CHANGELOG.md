# Changelog

All notable changes to this template. Versions follow [Semantic Versioning](https://semver.org):
a **major** version breaks the projects created from the template (renamed conventions,
models, columns or macros), a **minor** one adds features they can adopt, a **patch** fixes.

Projects created from the template record the version they started from, so they can see
here what changed since.

## [1.0.0] - 2026-10-16

First stable version of the template.

### Structure and conventions
- Medallion layers bronze / silver / gold with the naming convention
  `<tb|vw|ep>_<brz|slv|gld>_l<n>_<domain>__<table>`, validated on every run
  (`validate_model_naming`): layer, domain, level dependencies and 63-character names.
- CTE-first SQL, explicit materialization in every model, enforced contracts in gold.
- Groups per domain, grants per layer and use case from environment variables, layer and
  domain tags.

### Warehouse independence
- Runs on BigQuery (production), Snowflake, Databricks, Redshift, Postgres and DuckDB,
  selected with `DBT_ADAPTER`: one profile per warehouse, the `data_types` var with
  `data_type()`, physical layout and incremental strategy per warehouse
  (`config_by_date`), dbt cross-database macros.

### Examples
- Seed, check and timestamp snapshots, incremental model with a reprocessing window,
  Python models in silver (anomaly detection) and gold (forecast), an alert prepared by dbt,
  analyses, singular and generic tests.

### Testing
- Unit tests for the business rules, data tests with `dbt_utils` and `dbt_checks`,
  `validate_model_documentation` and `validate_exposures`, pytest for the DAG helpers.
- `scripts/ci_test.sh`: the whole suite on DuckDB with test data, no warehouse needed.

### Orchestration
- Exposures as the scheduling contract: schedule tag (`daily`, `weekly`, `monthly` or
  `custom` with `meta.cron`), `ready_by`, `notify`, `criticality`.
- Airflow DAGs generated from them (Airflow 2 and 3): freshness, snapshot, build, failure
  routing through the lineage, deadline checks independent of the build, alerts.

### CI/CD and tooling
- Cloud Build pipelines for pull requests (with optional slim CI) and deployment (manifest,
  docs, Composer bucket, state for the next slim CI); `drop_pr_schemas` to clean PR schemas.
- `scripts/new_domain.py`, CODEOWNERS, pull request template, CONTRIBUTING, pre-commit,
  SQLFluff, `.editorconfig`.
