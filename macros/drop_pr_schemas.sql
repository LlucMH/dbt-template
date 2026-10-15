{#
    drop_pr_schemas

    Drops the schemas created by the slim CI of pull requests (cloudbuild-pr.yaml builds
    in schemas suffixed with _pr_<number>, which nothing drops automatically).
    Warehouse-agnostic: dbt's list_schemas macro and adapter.drop_schema.

    Safety:
      - dry_run is true by default: it only lists what it would drop;
      - only schemas ending exactly in _pr_<number> are ever touched;
      - it refuses to run on a shared target (var shared_targets: ideacion, prod),
        whose schemas are never PR schemas.

    Usage (with the target the PRs build on, e.g. local):
        dbt run-operation drop_pr_schemas                                    # list them
        dbt run-operation drop_pr_schemas --args '{dry_run: false}'          # drop all of them
        dbt run-operation drop_pr_schemas --args '{pr_number: 42, dry_run: false}'   # only PR 42

    Run it periodically (e.g. weekly) or when a pull request is closed.
#}
{% macro drop_pr_schemas(pr_number=none, dry_run=true) %}
    {%- if target.name in var('shared_targets', ['ideacion', 'prod']) -%}
        {{ exceptions.raise_compiler_error("drop_pr_schemas never runs on a shared target (" ~ target.name ~ ")") }}
    {%- endif -%}

    {%- set suffix = '_pr_' ~ pr_number if pr_number is not none else '_pr_[0-9]+' -%}
    {%- set pattern = '^.+' ~ suffix ~ '$' -%}
    {%- set schemas = [] -%}
    {#- dbt's list_schemas macro: implemented by every adapter, returns a one-column table -#}
    {%- for schema in list_schemas(target.database).columns[0].values() -%}
        {%- if modules.re.match(pattern, schema, modules.re.IGNORECASE) -%}
            {%- do schemas.append(schema) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if not schemas -%}
        {% do log('drop_pr_schemas: no PR schemas found in ' ~ target.database, info=True) %}
    {%- endif -%}
    {%- for schema in schemas | sort -%}
        {%- if dry_run -%}
            {% do log('drop_pr_schemas (dry run): would drop ' ~ target.database ~ '.' ~ schema, info=True) %}
        {%- else -%}
            {% do adapter.drop_schema(api.Relation.create(database=target.database, schema=schema)) %}
            {% do log('drop_pr_schemas: dropped ' ~ target.database ~ '.' ~ schema, info=True) %}
        {%- endif -%}
    {%- endfor -%}
{% endmacro %}
