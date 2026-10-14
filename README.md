# poc_dbt

**Warehouse-agnostic** dbt Core project template, so that every team creates projects
with the same shape: same layers, same names, same documentation and same tests,
whatever engine they run on. Orchestrated with Composer (Airflow) and deployed with
Cloud Build.

| Warehouse | Use |
|---|---|
| BigQuery | Production (default) |
| Snowflake, Databricks, Redshift, Postgres | Supported with the same code |
| DuckDB | Local development and CI: full `dbt build` with no warehouse and no cost |

## Getting started

Requirements: Python 3.10–3.12.

```bash
python -m venv .venv
source .venv/bin/activate                      # PowerShell: .venv\Scripts\Activate.ps1

pip install -r requirements-dev.txt            # BigQuery + DuckDB + lint and hooks
# Another warehouse: pip install -r requirements/<adapter>.txt sqlfluff pre-commit

cp .env.example .env                           # choose DBT_ADAPTER and fill in its section
set -a; . ./.env; set +a                       # load variables (PowerShell: see .env.example)

dbt deps
dbt debug                                      # checks connection and profile
```

For BigQuery, also: `gcloud auth application-default login`.

### Try it without a warehouse (DuckDB)

```bash
export DBT_ADAPTER=duckdb DBT_TARGET=ci        # PowerShell: $env:DBT_ADAPTER='duckdb'; $env:DBT_TARGET='ci'
mkdir -p target_ci
dbt deps
dbt run-operation load_ci_landing_data         # creates the landing tables with test data
dbt build
```

This is exactly what the CI runs. The 4 `relationships` warnings are expected: the test
data deliberately includes broken relationships to check they are detected.

## Dependencies

| File | Contents | Who installs it |
|---|---|---|
| [requirements.txt](requirements.txt) | dbt-core + production adapter (BigQuery) | Composer, Cloud Build, local |
| [requirements-dev.txt](requirements-dev.txt) | `requirements.txt` + DuckDB + SQLFluff + pre-commit | Local development and CI |
| [requirements/](requirements/)`<adapter>.txt` | dbt-core + adapter for Snowflake, Databricks, Redshift or Postgres | Only environments running on that warehouse |

Each file pins dbt-core and its adapter together, in versions tested with each other.

## How warehouse independence works

Models use standard SQL. Everything warehouse-specific lives in four places:

| Piece | Where | What it solves |
|---|---|---|
| Connection | [profiles.yml](profiles.yml) | One profile per warehouse (`poc_dbt_<DBT_ADAPTER>`), all with the same targets |
| Data types | var `data_types` in [dbt_project.yml](dbt_project.yml) + macro [data_type()](macros/data_type.sql) | Physical type of each logical type (`string`, `integer`, `float`, `numeric`, `date`, `timestamp`, `boolean`) |
| Physical layout and incremental | [macros/config_by_date.sql](macros/config_by_date.sql) | Partition / clustering / sort key / index and incremental strategy of each warehouse |
| SQL functions | dbt cross-database macros (`dbt.safe_cast`, `dbt.dateadd`, `dbt.date_trunc`...) | Functions whose syntax differs per engine |

Rules to keep the SQL portable:

- **Types**: never type names in SQL. `CAST(x AS {{ data_type('string') }})`; in YAML files,
  `data_type: "{{ var('data_types')[target.type].string }}"` (YAML files cannot call macros).
- **Engine-specific functions** (`SAFE_DIVIDE`, `IF()`, `QUALIFY`, `DATE_SUB`...): no.
  Use their standard equivalents (`x / NULLIF(y, 0)`, `CASE WHEN`, `ROW_NUMBER()` in a CTE)
  or the matching `dbt.*` macro.
- **Divisions**: cast to `data_type('float')` before dividing (integer / integer truncates
  in Postgres and Redshift).
- **Gold**: every column is cast to its contract type, so the contract holds on every engine.
- **Test filters**: macros cannot be used in the `where` of a test; use the placeholder
  `__test_window_start_date__` (see [macros/get_where_subquery.sql](macros/get_where_subquery.sql)).
- **Names**: 63 characters at most (the Postgres limit); the validator checks it.

### Adding another warehouse

1. Its profile in `profiles.yml` (`poc_dbt_<adapter>`, with targets `local`, `ideacion` and `prod`).
2. Its entry in the `data_types` var of `dbt_project.yml`.
3. Its branch in `macros/config_by_date.sql` (otherwise the generic one is used: `delete+insert` with no physical layout).
4. `requirements/<adapter>.txt` with dbt-core and the adapter.
5. Test it with `dbt build` against the warehouse.

### Known limitations

- `dbt.safe_cast` is a plain `CAST` in Postgres and Redshift: a malformed source value makes
  the query fail instead of returning NULL.
- `+labels` (BigQuery) and `+query_tag` (Snowflake) are only used by their warehouse; others ignore them.
- In Snowflake, the physical names of sources are quoted (they may contain hyphens), so they
  are case-sensitive.

## Configuration

All per-environment configuration is environment variables, documented per warehouse in
[.env.example](.env.example). `profiles.yml` is versioned because it holds no secrets.

| Variable | Use | Default |
|---|---|---|
| `DBT_ADAPTER` | Warehouse: `bigquery`, `snowflake`, `databricks`, `redshift`, `postgres`, `duckdb` | `bigquery` |
| `DBT_TARGET` | Target: `local`, `ideacion`, `prod` (and `ci` on DuckDB) | `local` |
| `DBT_USER` | Suffix of personal schemas outside `ideacion`/`prod` | `local` |
| `DBT_SOURCE_DATABASE` | Database / project / catalog of the landing tables | the target's |
| `DBT_THREADS` | Parallel threads | `4` |
| `DBT_BQ_*`, `DBT_SNOWFLAKE_*`, `DBT_DATABRICKS_*`, `DBT_REDSHIFT_*`, `DBT_POSTGRES_*`, `DBT_DUCKDB_PATH` | Connection of each warehouse | see `.env.example` |
| `DBT_GIT_REPO_URL` | Git repository of the project | to be created |
| `DBT_GCS_BUCKET`, `DBT_GCS_PATH` | Deployment destination (informational locally) | Composer bucket |

dbt variables in [dbt_project.yml](dbt_project.yml) (`vars:`):

- `data_types`: data types per warehouse.
- `shared_targets`: targets that write to the exact schemas (`ideacion`, `prod`).
- `max_name_length`: maximum name length (63).
- `test_window_days`: days covered by tests using `__test_window_start_date__` (7).
- `business_entity_2_reprocess_days`: days reprocessed by the example incremental model (3).

### Schemas per target

| Target | Schema of a model with `+schema: ds_gld_use_case_1` |
|---|---|
| `ideacion`, `prod` | `ds_gld_use_case_1` |
| any other | `ds_gld_use_case_1_<DBT_USER>` |

In BigQuery a schema is a dataset.

## Structure

| Folder | Contents | Schema |
|---|---|---|
| `models/bronze/<source_database>/` | One model per source table: renaming, casting, cleaning. The only layer using `source()` | `ds_brz_<domain>` |
| `models/silver/<business_entity>/` | Reusable transformations and business logic | `ds_slv_<domain>` |
| `models/gold/<use_case>/` | Final products per use case, with contract, `meta` for Airflow and exposures | `ds_gld_<domain>` |
| `seeds/<domain>/` | Small, static reference data maintained by hand | `ds_seeds` |
| `snapshots/<source_database>/` | SCD2 history of tables the source overwrites | `ds_snapshots` |
| `analyses/<domain>/` | Versioned ad hoc queries (only compiled) | — |
| `tests/<layer>/` | Singular tests | `ds_test_failures` |
| `tests/generic/` | Custom generic tests | — |
| `macros/ci/` | Test data for the CI (`ci` target only) | — |
| `requirements/` | Dependencies for warehouses other than BigQuery | — |
| `sqlfluff_libs/` | Macro stubs for linting with SQLFluff | — |

Model names: `<tb|vw|ep>_<brz|slv|gld>_l<n>_<domain>__<table>`. Validated by
[macros/validate_model_naming.sql](macros/validate_model_naming.sql) at the start of every
run. Every model declares its materialization explicitly in its `config()`, and the prefix
must match it. The full conventions are in the header of the YAML files of each folder.

Model SQL follows the **CTE-first** style:

```sql
{{ config(materialized='view') }}

WITH table_1 AS (                   -- 1. import CTEs: one per ref()/source(),
    SELECT field_1, field_2         --    only the columns that are used
    FROM {{ ref('vw_brz_l0_source_database_1__table_1') }}
),

valid_records AS (                  -- 2. logic CTEs, named after what they do
    ...
),

final AS (                          -- 3. output columns, with _dbt_run_at
    SELECT ..., {{ dbt_run_at() }} AS _dbt_run_at
    FROM valid_records
)

SELECT * FROM final
```

`ref()` and `source()` only appear in import CTEs, so dependencies are visible at the top of
the file. Incremental filters go in the import CTE, so the warehouse reads less data.

### Examples included

| Type | Example | Shows |
|---|---|---|
| Seed | `business_entity_1__table_2_group_map` | Manual mapping, typed per warehouse, consumed by a silver l0 model |
| Snapshot (check) | `snp_source_database_1__table_2` | Column comparison, deletions as a new version (`hard_deletes: new_record`) |
| Snapshot (timestamp) | `snp_source_database_2__table_1` | `updated_at` comparison, deletions close the version (`hard_deletes: invalidate`) |
| Incremental model | `tb_slv_l1_business_entity_2__fact_table_3_partitioned` | Reprocessing window, layout and strategy per warehouse |
| Singular test | `tests/silver/...daily_agg_matches_fact`, `tests/gold/...monthly_matches_daily` | Reconciliation between layers and between reports |
| Generic test | `tests/generic/not_in_future.sql` | Reusable test applied from the YAML |
| Analysis | `business_entity_1__group_mapping_gaps` | Gaps in the seed before updating it |
| Analysis | `business_entity_1__table_2_change_history` | Changes per day from the SCD2, to size the snapshot frequency |
| Analysis | `business_entity_2__incremental_drift` | Records the incremental table missed (late arrivals outside the window) |

## Tags

| Tag | Applied to | Use |
|---|---|---|
| `brz`, `slv`, `gld` | Every model of each layer | `dbt build --select tag:slv` |
| `<source_database>`, `<business_entity>`, `<use_case>` | Every model, seed and snapshot of each domain | `dbt build --select tag:use_case_1` |
| `seed`, `snapshot` | Every seed / snapshot | `dbt build --select tag:snapshot` |
| `<use_case>` + `daily` / `weekly` / `monthly` | Exposures: use case and how often it must be refreshed | `dbt build --select +tag:daily` |

To build everything the exposures of one use case and schedule need:

```bash
dbt build --select "+tag:use_case_2,+tag:monthly"
```

## Common commands

```bash
dbt build --select <model>+                # run and test a model and everything downstream
dbt build --select tag:use_case_1          # a whole use case
dbt build --select +tag:daily              # everything the daily exposures need
dbt run-operation validate_model_naming    # validate names without running anything
sqlfluff lint models analyses tests        # SQL style (ansi dialect)
dbt compile --select <analysis>            # compile an analysis (SQL in target/compiled/)
dbt snapshot                               # update snapshots (never --full-refresh in ideacion/prod)
```

## Deployment

[cloudbuild.yaml](cloudbuild.yaml):

1. **test**: lint and full `dbt build` on DuckDB with test data, with a second pass for the
   incremental model and the snapshots. If anything fails, nothing is deployed.
2. **manifest**: installs the adapter of the deployment warehouse (`_DBT_ADAPTER`) and builds
   `target/manifest.json` with the deployment target, which is what Airflow reads.
3. **deploy**: syncs the folder to `gs://<_DBT_GCS_BUCKET>/<_DBT_GCS_PATH>`. The sync
   **deletes from the bucket** whatever no longer exists in the project.

Warehouse, target, bucket, path and region are trigger substitutions. For a warehouse other
than BigQuery, its connection variables must reach the `manifest` step (ideally as
`secretEnv` from Secret Manager).

## To do

- Create the git repository, fill in `DBT_GIT_REPO_URL` and enable the hooks:
  `pre-commit install`.
- Replace the placeholders (`table_1`, `field_1`, `...`) with real tables and columns.
- Test `dbt build` against every real warehouse that will be used (see *Known limitations*).
