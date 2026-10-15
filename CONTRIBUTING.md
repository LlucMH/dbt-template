# Contributing to poc_dbt

How to make a change to this project, from a branch to production. The conventions
themselves are in the [README](README.md) and in the header of the YAML files of each folder.

## Workflow

1. **Branch** from `main`: `feature/<domain>-<short-description>` or `fix/<...>`.
2. **Develop locally** against your personal schemas (`DBT_TARGET=local`, `DBT_USER=<you>`):
   ```bash
   dbt build --select <model>+
   ```
   Or without a warehouse, on DuckDB (see the README).
3. **Check before pushing**, the same suite the CI runs:
   ```bash
   bash scripts/ci_test.sh
   ```
4. **Open a pull request** to `main` and fill in the template. The PR pipeline
   (`cloudbuild-pr.yaml`) runs the suite and, if enabled, builds what you changed on the
   real warehouse in schemas suffixed with the PR number. CODEOWNERS of the touched
   domains are asked for review.
5. **Merge**: the deployment pipeline (`cloudbuild.yaml`) tests again, builds the manifest,
   publishes the docs and deploys to Composer. The Airflow DAGs pick the change up in their
   next run.

## Adding things

| To add | How |
|---|---|
| A source database, business entity or use case | `python scripts/new_domain.py --layer <bronze\|silver\|gold> --name <name>` and paste the printed block into `dbt_project.yml` |
| A model | A `.sql` (or `.py`) file named `<tb\|vw\|ep>_<brz\|slv\|gld>_l<n>_<domain>__<table>` in the domain folder, with an explicit `config(materialized=...)`, documented in the domain's models YAML |
| A business rule | Its unit test in `_<domain>__unit_tests.yml` |
| A data expectation | A data test in the models YAML: dbt built-ins, `dbt_utils`, `dbt_checks`, or a generic test in `tests/generic/` |
| A consumer (dashboard, file, alert) | An exposure in the use case's `_<use_case>__exposures.yml`, tagged with the use case and its schedule (`daily`, `weekly`, `monthly`) |
| A Python model | See *Python models* in the README: `python` tag, `enabled` on Python-capable warehouses, small inputs, no side effects |

## Rules that the CI enforces

- Naming convention, layer and level dependencies, name length (`validate_model_naming`).
- Description on every model and column, primary key test, `_dbt_run_at`, `python` tag on
  Python models (`validate_model_documentation`).
- SQL style (SQLFluff), unit tests, data tests, contracts in gold.

## Rules that the review enforces

- No business logic in macros: macros are for technical, reusable logic.
- No side effects in models (files, e-mails, API calls): dbt prepares the data in a gold
  table, Airflow acts on it (see `dags/`).
- Renaming or removing a gold model or column is a breaking change for its consumers: say
  so in the pull request and notify the exposure owners.
- Never `--full-refresh` or drop a snapshot in `ideacion` or `prod`: its history cannot be
  rebuilt.
