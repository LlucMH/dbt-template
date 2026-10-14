{#
    duckdb__safe_cast

    dbt-duckdb does not implement safe_cast and falls back to a plain CAST, which
    fails on a malformed value. DuckDB does have TRY_CAST, which returns NULL like
    SAFE_CAST (BigQuery) or TRY_CAST (Snowflake, Databricks). Enabled by the
    dispatch block in dbt_project.yml.

    Postgres and Redshift have no native equivalent: there dbt.safe_cast is a
    plain CAST, and a malformed source value makes the query fail.
#}
{% macro duckdb__safe_cast(field, type) -%}
    TRY_CAST({{ field }} AS {{ type }})
{%- endmacro %}
