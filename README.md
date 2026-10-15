# poc_dbt

**Warehouse-agnostic** dbt Core project template, so that every team creates projects
with the same shape: same layers, same names, same documentation, same tests and the same
path to production, whatever engine they run on. Orchestrated with Composer (Airflow) and
deployed with Cloud Build.

| Warehouse | Use |
|---|---|
| BigQuery | Production (default) |
| Snowflake, Databricks, Redshift, Postgres | Supported with the same code |
| DuckDB | Local development and CI: full `dbt build` with no warehouse and no cost |

To contribute, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Getting started

Requirements: Python 3.10–3.12.

```bash
python -m venv .venv
source .venv/bin/activate                      # PowerShell: .venv\Scripts\Activate.ps1

pip install -r requirements-dev.txt            # BigQuery + DuckDB + lint, hooks and pytest
# Another warehouse: pip install -r requirements/<adapter>.txt sqlfluff pre-commit pytest

cp .env.example .env                           # choose DBT_ADAPTER and fill in its section
set -a; . ./.env; set +a                       # load variables (PowerShell: see .env.example)

dbt deps
dbt debug                                      # checks connection and profile
```

For BigQuery, also: `gcloud auth application-default login`.

### Try it without a warehouse (DuckDB)

```bash
bash scripts/ci_test.sh
```

The same suite the CI runs: lint, pytest, the naming and documentation rules, and a full
`dbt build` on DuckDB with test data, plus a second incremental pass. The 4 `relationships`
warnings are expected: the test data deliberately includes broken relationships.

## Dependencies

| File | Contents | Who installs it |
|---|---|---|
| [requirements.txt](requirements.txt) | dbt-core + production adapter (BigQuery) | Composer, Cloud Build, local |
| [requirements-dev.txt](requirements-dev.txt) | `requirements.txt` + DuckDB + SQLFluff + pre-commit + pytest | Local development and CI |
| [requirements/](requirements/)`<adapter>.txt` | dbt-core + adapter for Snowflake, Databricks, Redshift or Postgres | Only environments running on that warehouse |
| [packages.yml](packages.yml) | dbt packages: `dbt_utils` and [`dbt_checks`](https://github.com/LlucMH/dbt-checks) | `dbt deps` |

Each requirements file pins dbt-core and its adapter together, in versions tested with each other.

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
- Python models do not run on Postgres or Redshift (disabled there, with everything that
  depends on them) and need Dataproc Serverless on BigQuery.

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
| `DBT_GRANTEES_SILVER`, `DBT_GRANTEES_<USE_CASE>` | Who can read silver and each use case (see *Ownership and access*) | no grants |
| `DBT_BQ_*`, `DBT_SNOWFLAKE_*`, `DBT_DATABRICKS_*`, `DBT_REDSHIFT_*`, `DBT_POSTGRES_*`, `DBT_DUCKDB_PATH` | Connection of each warehouse | see `.env.example` |
| `DBT_GIT_REPO_URL` | Git repository of the project | to be filled in |
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
| `models/_groups.yml` | One group (owner) per domain | — |
| `seeds/<domain>/` | Small, static reference data maintained by hand | `ds_seeds` |
| `snapshots/<source_database>/` | SCD2 history of tables the source overwrites | `ds_snapshots` |
| `analyses/<domain>/` | Versioned ad hoc queries (only compiled) | — |
| `tests/<layer>/` | Singular tests | `ds_test_failures` |
| `tests/generic/` | Custom generic tests | — |
| `macros/ci/` | Test data for the CI (`ci` target only) | — |
| `dags/` | Airflow DAGs and their tests | — |
| `scripts/` | `ci_test.sh` (test suite) and `new_domain.py` (domain generator) | — |
| `requirements/` | Dependencies for warehouses other than BigQuery | — |
| `sqlfluff_libs/` | Macro stubs for linting with SQLFluff | — |
| `.github/` | CODEOWNERS and pull request template | — |

Model names: `<tb|vw|ep>_<brz|slv|gld>_l<n>_<domain>__<table>`. Validated by
[macros/validate_model_naming.sql](macros/validate_model_naming.sql) at the start of every
run. Every model declares its materialization explicitly in its `config()`, and the prefix
must match it. The full conventions are in the header of the YAML files of each folder.

To add a source database, business entity or use case:

```bash
python scripts/new_domain.py --layer <bronze|silver|gold> --name <name> [--owner <team>]
```

It creates the folder, its YAML files and its group, and prints the block to paste into
`dbt_project.yml`.

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

### Python models

For logic that is awkward in portable SQL (statistics, forecasting, ML), a model can be a
`.py` file with a `model(dbt, session)` function. Conventions:

- **Where they run**: BigQuery (Dataproc Serverless: set `DBT_BQ_GCS_BUCKET`), Snowflake
  (Snowpark), Databricks (serverless cluster) and DuckDB, so the CI tests them too. Postgres
  and Redshift cannot run Python: the model YAML disables them there
  (`enabled: "{{ target.type not in ['postgres', 'redshift'] }}"`), and anything that
  depends on them must be disabled the same way.
- **Always tables**: dbt has no Python views, so their prefix is `tb_` (the prefix is the
  materialization, never the language).
- **`python` tag**: required (checked by `validate_model_documentation`), so they can be
  selected or excluded: `dbt build --exclude tag:python`.
- **One table, no side effects**: a Python model reads with `ref()`, transforms and returns
  one DataFrame. Files, e-mails, charts or API calls go to Airflow (see *Orchestration*).
- **Libraries**: declare them in `dbt.config(packages=[...])` (Snowflake, Databricks) and in
  `requirements-dev.txt` (DuckDB); on BigQuery, extra libraries need a custom Dataproc image.
  Pin exact versions, and fix the random seed of any stochastic computation.
- **Small inputs**: `to_pandas()` brings the input into memory. Read aggregated models; for
  large inputs, use the warehouse's native DataFrame API.
- **Same code on every warehouse**: `to_pandas()` converts whatever `dbt.ref()` returns
  (Snowpark, PySpark, DuckDB) and `finalize()` upper-cases columns on Snowflake. Both are
  copied in each model, because Python models cannot import project modules.
- **Dates**: pandas turns `DATE` into `datetime64`; convert back with `.dt.date` before
  returning, or the column is written as a timestamp.
- **No contracts and no unit tests**: dbt supports neither on Python models; a gold Python
  model disables the inherited contract in its YAML.
- **`_dbt_run_at`**: Python models cannot read `run_started_at`; they use the time they ran.

## Testing

| Kind | Where | Checks |
|---|---|---|
| Unit tests | `models/<layer>/<domain>/_<domain>__unit_tests.yml` | **Logic**, with fixed inputs and expected outputs: one per business rule |
| Data tests | Models YAML: dbt built-ins, `dbt_utils`, `dbt_checks.*` | **Data**: keys, relationships, ranges, ratios, freshness of dates, non-empty gold |
| Generic tests | `tests/generic/` | Reusable checks a package does not provide (e.g. `no_date_gaps`) |
| Singular tests | `tests/<layer>/` | Reconciliations between layers and between reports |
| Rules | `validate_model_naming`, `validate_model_documentation` | Naming, layers, name length; descriptions, primary keys, `_dbt_run_at`, `python` tag |
| DAG helpers | `dags/tests/` (pytest) | Failure routing, e-mail content, `dbt show` parsing |

All of them run in [scripts/ci_test.sh](scripts/ci_test.sh). Unit tests of incremental models
read the existing table (`this`), so on an empty database the incremental model is first
created with `dbt run --empty`.

## Ownership and access

- **Groups** ([models/_groups.yml](models/_groups.yml)): every model, seed and snapshot belongs
  to the group of its domain, owned by its team. [.github/CODEOWNERS](.github/CODEOWNERS) uses
  the same teams for reviews.
- **Grants** (`+grants` in `dbt_project.yml`): `SELECT` on silver for the analysts
  (`DBT_GRANTEES_SILVER`) and on each use case for its consumers (`DBT_GRANTEES_<USE_CASE>`);
  bronze is internal. Grantees are comma-separated, in the warehouse's format (BigQuery:
  `group:bi@company.com`; Snowflake, Postgres, Redshift: roles; Databricks: groups). An empty
  variable applies no grants at all: an empty list would make dbt revoke the existing ones.

## Tags

| Tag | Applied to | Use |
|---|---|---|
| `brz`, `slv`, `gld` | Every model of each layer | `dbt build --select tag:slv` |
| `<source_database>`, `<business_entity>`, `<use_case>` | Every model, seed and snapshot of each domain | `dbt build --select tag:use_case_1` |
| `seed`, `snapshot`, `python` | Every seed / snapshot / Python model | `dbt build --exclude tag:python` |
| `<use_case>` + `daily` / `weekly` / `monthly` | Exposures: use case and how often it must be refreshed | `dbt build --select +tag:daily` |

To build everything the exposures of one use case and schedule need:

```bash
dbt build --select "+tag:use_case_2,+tag:monthly"
```

## Orchestration

[dags/poc_dbt_dags.py](dags/poc_dbt_dags.py) creates one Airflow DAG per schedule used by the
exposures (`poc_dbt__daily`, `poc_dbt__monthly`...), from the deployed `target/manifest.json`:

```
prepare -> source_freshness -> [snapshot, daily only] -> build (+tag:<schedule>) -> notify_failures
                                                                                -> send_anomaly_alert (daily)
```

- **source_freshness**: a source past its `error_after` stops the DAG before building on stale data.
- **notify_failures**: e-mails each team (`meta.notify`) the nodes it owns that failed, from
  `run_results.json`; `[HIGH]` in the subject when one of them is `criticality: high`.
- **send_anomaly_alert**: e-mails the rows of `vw_gld_l0_use_case_2__amount_anomaly_alerts`,
  only when it returns rows. dbt prepares *what* to send; Airflow sends it.
- **Recipients**: Airflow Variable `dbt_notify_recipients`, JSON `{team: [e-mails]}`. E-mail
  addresses never live in the repository.
- **Composer environment**: `DBT_ADAPTER`, `DBT_TARGET=prod` and the warehouse connection
  variables. Optional `DBT_BIN` (dbt in a dedicated virtualenv, to keep its dependencies apart
  from Airflow's) and `DBT_RUNS_DIR` (artifacts of each run, shared by every worker).

[.airflowignore](.airflowignore) keeps Airflow away from everything but `dags/`.

## Common commands

```bash
dbt build --select <model>+                # run and test a model and everything downstream
dbt build --select tag:use_case_1          # a whole use case
dbt build --select +tag:daily              # everything the daily exposures need
dbt test --select test_type:unit           # unit tests only
dbt run-operation validate_model_naming    # naming rules, without running anything
dbt run-operation validate_model_documentation   # documentation rules
sqlfluff lint models analyses tests        # SQL style (ansi dialect)
dbt compile --select <analysis>            # compile an analysis (SQL in target/compiled/)
dbt snapshot                               # update snapshots (never --full-refresh in ideacion/prod)
bash scripts/ci_test.sh                    # the whole CI suite on DuckDB
```

## CI/CD

| Pipeline | Trigger | Steps |
|---|---|---|
| [cloudbuild-pr.yaml](cloudbuild-pr.yaml) | Pull request to `main` | `ci_test.sh` on DuckDB; optional **slim CI** (`_SLIM_CI=true`): `dbt build --select state:modified+ --defer` on the real warehouse, in schemas suffixed `_pr_<n>`, against the last deployed manifest |
| [cloudbuild.yaml](cloudbuild.yaml) | Push to `main` | `ci_test.sh`; manifest with the deployment target; docs (`dbt docs generate --static`, if `_DBT_DOCS_BUCKET` is set); deploy to `gs://<_DBT_GCS_BUCKET>/<_DBT_GCS_PATH>`; publish the manifest for the next slim CI and the docs page |

The deployment **deletes from the bucket** whatever no longer exists in the project. Warehouse,
target, buckets, paths and region are trigger substitutions. For a warehouse other than
BigQuery, its connection variables must reach the steps that talk to it (ideally as
`secretEnv` from Secret Manager).

## Examples included

| Type | Example | Shows |
|---|---|---|
| Seed | `business_entity_1__table_2_group_map` | Manual mapping, typed per warehouse, consumed by a silver l0 model |
| Snapshot (check) | `snp_source_database_1__table_2` | Column comparison, deletions as a new version (`hard_deletes: new_record`) |
| Snapshot (timestamp) | `snp_source_database_2__table_1` | `updated_at` comparison, deletions close the version (`hard_deletes: invalidate`) |
| Incremental model | `tb_slv_l1_business_entity_2__fact_table_3_partitioned` | Reprocessing window, layout and strategy per warehouse |
| Python model (silver) | `tb_slv_l3_business_entity_2__agg_table_3_daily_anomalies` | Anomaly flag with a rolling z-score over 7 calendar days (pandas) |
| Python model (gold) | `tb_gld_l2_use_case_2__monthly_amount_forecast` | Actual months plus a 3-month linear-trend forecast (numpy), without contract |
| Alert | `vw_gld_l0_use_case_2__amount_anomaly_alerts` + exposure + DAG task | dbt prepares the alert, Airflow sends it only when there is something to report |
| Unit tests | `_<domain>__unit_tests.yml` | Latest load wins, unmapped values, aggregation by group, incremental window (`this` + `is_incremental`) |
| Singular tests | `tests/silver/...daily_agg_matches_fact`, `tests/gold/...monthly_matches_daily` | Reconciliation between layers and between reports |
| Generic test | `tests/generic/no_date_gaps.sql` | Reusable test applied from the YAML |
| Analysis | `business_entity_1__group_mapping_gaps` | Gaps in the seed before updating it |
| Analysis | `business_entity_1__table_2_change_history` | Changes per day from the SCD2, to size the snapshot frequency |
| Analysis | `business_entity_2__incremental_drift` | Records the incremental table missed (late arrivals outside the window) |

## To do

- Mark the GitHub repository as a *Template repository* (Settings → General).
- Fill in `DBT_GIT_REPO_URL`, enable the hooks (`pre-commit install`) and replace the
  placeholder owners of `.github/CODEOWNERS` with the real GitHub teams.
- Replace the placeholders (`table_1`, `field_1`, `...`) with real tables and columns.
- Create the Cloud Build triggers (PR and `main`), the Composer variables and the Airflow
  Variable `dbt_notify_recipients`.
- Drop the `_pr_<n>` schemas of the slim CI periodically.
- Test `dbt build` against every real warehouse that will be used (see *Known limitations*).
