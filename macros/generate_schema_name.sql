{#
    generate_schema_name

    Resolves the schema of every node (dataset in BigQuery, schema elsewhere).

    - shared targets (var shared_targets, ideacion and prod by default):
      the exact schema set in +schema (e.g. ds_gld_use_case_1).
    - any other target (local, ci, sandbox...): the user is appended as a suffix
      so that several developers do not overwrite each other (ds_gld_use_case_1_u0123456).

    The user comes from the DBT_USER environment variable and is sanitized:
    lowercase, and any character outside [a-z0-9_] becomes "_".
    If a node does not set +schema, target.schema is used.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set base_schema = custom_schema_name | trim if custom_schema_name else target.schema -%}
    {%- set shared_targets = var('shared_targets', ['ideacion', 'prod']) -%}

    {%- if target.name in shared_targets -%}
        {{ base_schema }}
    {%- else -%}
        {%- set raw_user = env_var('DBT_USER', 'local') | lower -%}
        {%- set user = modules.re.sub('[^a-z0-9_]', '_', raw_user) -%}
        {{ base_schema }}_{{ user }}
    {%- endif -%}

{%- endmacro %}
