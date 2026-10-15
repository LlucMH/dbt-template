## What and why

<!-- What this pull request changes and the business reason behind it. -->

## Type of change

- [ ] New model / domain
- [ ] Change to an existing model (logic, columns, materialization)
- [ ] Tests or documentation only
- [ ] Platform (macros, CI/CD, DAGs, configuration)

## Checklist

- [ ] Model names follow `<tb|vw|ep>_<brz|slv|gld>_l<n>_<domain>__<table>` and the materialization is explicit in `config()`.
- [ ] Every new or changed model and column is documented, with a primary key test and `_dbt_run_at`.
- [ ] Business rules have a unit test; data expectations have data tests (dbt-checks where possible).
- [ ] SQL is warehouse-agnostic: types through `data_type()`, no engine-specific functions.
- [ ] Gold contract updated (`data_type` of every column) if gold columns changed.
- [ ] `bash scripts/ci_test.sh` passes locally.

## Impact on consumers

- [ ] No renamed or removed gold models / columns.
- [ ] Renamed or removed gold models / columns: consumers notified (list the exposures):

<!-- Exposures affected, migration steps, need for a --full-refresh... -->
