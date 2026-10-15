"""Creates a new domain of the template: a source database (bronze), a business
entity (silver) or a use case (gold).

    python scripts/new_domain.py --layer bronze --name crm
    python scripts/new_domain.py --layer silver --name customer
    python scripts/new_domain.py --layer gold   --name churn_report --owner team_churn

It creates the domain folder with its YAML files (same headers and conventions as the
existing domains), adds the domain group to models/_groups.yml, and prints the block
to paste into dbt_project.yml (under models > poc_dbt > <layer>). It never overwrites
an existing file.

Next steps after running it: add the models (.sql) with names
<tb|vw|ep>_<brz|slv|gld>_l<n>_<name>__<table>, document them in the models YAML, and run
    dbt run-operation validate_model_naming
    dbt run-operation validate_model_documentation
"""
import argparse
import re
import sys
from pathlib import Path

LAYERS = {
    "bronze": {"prefix": "brz", "domain": "source database"},
    "silver": {"prefix": "slv", "domain": "business entity"},
    "gold": {"prefix": "gld", "domain": "use case"},
}
NAME_PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")
# Longest model name the domain allows: <tb>_<brz>_l<n>_<name>__<table> within 63 characters.
MAX_NAME_LENGTH = 30

MODELS_HEADER = """# =============================================================================
# {title} models of {name}
#
# Template conventions: see the header of the models YAML of any existing
# {layer} domain. In short:
#   - Model name: <tb|vw|ep>_{prefix}_l<n>_{name}__<table>
#     (naming validated by macros/validate_model_naming.sql).
#   - Every model and every exposed column is documented, with a primary key
#     test (unique + not_null) and the _dbt_run_at column
#     (validated by macros/validate_model_documentation.sql).
#   - Materialization is explicit in each model's config().
# =============================================================================
version: 2

models: []
"""

SOURCES = """# =============================================================================
# Sources of {name}
#
# Template conventions: see models/bronze/source_database_1/_source_database_1__sources.yml.
#   - name = clean logical name; identifier = real physical name in the warehouse.
#   - Only freshness and not_null on keys here; uniqueness and relationships go
#     in the models YAML.
# =============================================================================
version: 2

sources:
  - name: {name}
    description: ...
    database: "{{{{ env_var('DBT_SOURCE_DATABASE', target.database) }}}}"
    quoting:
      identifier: true
    schema: ds_lnd_{name}
    loader: airflow

    config:
      tags: ["{name}"]
      meta:
        owner: {owner}
        source_system: {name}
      loaded_at_field: _ingestion_timestamp
      freshness:
        warn_after: {{count: 24, period: hour}}
        error_after: {{count: 48, period: hour}}

    tables: []
    # - name: <table>
    #   identifier: <physical table name>
    #   description: ...
    #   columns:
    #     - name: <key column>
    #       description: ...
    #       data_tests:
    #         - not_null
"""

EXPOSURES = """# =============================================================================
# Exposures of {name}
#
# Template conventions: see models/gold/use_case_1/_use_case_1__exposures.yml.
#   - One exposure per real consumer (dashboard, file, application, alert).
#   - Orchestration contract, read by the Airflow DAGs (dags/) and checked by
#     macros/validate_exposures.sql:
#       tags: the use case and ONE schedule: daily | weekly | monthly | custom
#       meta.cron: custom only (UTC; "2#3" = third Tuesday)
#       meta.ready_by ("HH:MM" UTC), meta.notify (team), meta.criticality (low | medium | high)
#   - owner: team identifier, never real email addresses.
# =============================================================================
version: 2

exposures: []
# - name: {name}__<consumer>
#   label: ...
#   type: dashboard | application | notebook | analysis | ml
#   maturity: low | medium | high
#   description: ...
#   owner:
#     name: {owner}
#   depends_on:
#     - ref('<gold model>')
#   config:
#     tags: ["{name}", "daily"]          # or "custom" + meta.cron
#     meta:
#       use_case: {name}
#       ready_by: "08:00"
#       notify: {owner}
#       criticality: medium
"""

PROJECT_BLOCK = {
    "bronze": """      {name}:
        +tags: {name}
        +group: {name}
        +schema: ds_brz_{name}
""",
    "silver": """      {name}:
        +tags: {name}
        +group: {name}
        +schema: ds_slv_{name}
""",
    "gold": """      {name}:
        +tags: {name}
        +group: {name}
        +schema: ds_gld_{name}
        # Gold: readable by the consumers of this use case (BI, export service...).
        +grants: "{{{{ {{'select': env_var('{grantees}', '').split(',') | map('trim') | list}} if env_var('{grantees}', '') else {{}} }}}}"
        +labels:
          use_case: {name}
        +meta:
          use_case: {name}
          owner: {owner}
          criticality: medium      # low | medium | high
          notify: {owner}
""",
}

GROUP = """  - name: {name}
    owner:
      name: {owner}
"""


def write_new(path, content, created):
    if path.exists():
        print(f"  exists, not overwritten: {path}")
        return
    path.write_text(content, encoding="utf-8")
    created.append(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--layer", required=True, choices=LAYERS)
    parser.add_argument("--name", required=True, help="domain name in snake_case, e.g. crm, customer, churn_report")
    parser.add_argument("--owner", help="owning team identifier (default: team_<name>); never an e-mail address")
    parser.add_argument("--project-dir", default=Path(__file__).resolve().parents[1], type=Path)
    args = parser.parse_args()

    name, layer = args.name, args.layer
    if not NAME_PATTERN.match(name) or "__" in name:
        sys.exit(f"Invalid name '{name}': snake_case, starting with a letter, without '__'.")
    if len(name) > MAX_NAME_LENGTH:
        sys.exit(f"Name '{name}' is too long ({len(name)} > {MAX_NAME_LENGTH}): model names would exceed 63 characters.")
    owner = args.owner or f"team_{name}"
    info = LAYERS[layer]
    values = {"name": name, "owner": owner, "layer": layer, "prefix": info["prefix"],
              "title": layer.capitalize(), "grantees": f"DBT_GRANTEES_{name.upper()}"}

    project = args.project_dir
    domain_dir = project / "models" / layer / name
    domain_dir.mkdir(parents=True, exist_ok=True)
    created = []
    write_new(domain_dir / f"_{name}__models.yml", MODELS_HEADER.format(**values), created)
    if layer == "bronze":
        write_new(domain_dir / f"_{name}__sources.yml", SOURCES.format(**values), created)
    if layer == "gold":
        write_new(domain_dir / f"_{name}__exposures.yml", EXPOSURES.format(**values), created)

    groups = project / "models" / "_groups.yml"
    groups_text = groups.read_text(encoding="utf-8")
    if re.search(rf"^  - name: {re.escape(name)}$", groups_text, flags=re.M):
        print(f"  group '{name}' already in {groups}")
    else:
        groups.write_text(groups_text.rstrip("\n") + "\n" + GROUP.format(**values), encoding="utf-8")
        created.append(groups)

    print(f"New {info['domain']} '{name}' ({layer}):")
    for path in created:
        print(f"  written: {path.relative_to(project)}")
    print(f"\nPaste into dbt_project.yml, under models > poc_dbt > {layer}:\n")
    print(PROJECT_BLOCK[layer].format(**values))
    if layer == "gold":
        print(f"Grantees of this use case: environment variable {values['grantees']} (see .env.example).")
    print("Until the domain has its first model, dbt warns about an unused configuration path: expected.")


if __name__ == "__main__":
    main()
