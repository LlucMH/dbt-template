{#
    dbt_run_at

    Returns the start of the dbt run (run_started_at, UTC) as a timestamp
    literal of the active warehouse.

    run_started_at is used instead of CURRENT_TIMESTAMP() because in a view
    CURRENT_TIMESTAMP() is evaluated when the view is queried, not when dbt runs.
    With the literal, views and tables store the actual run time, identical for
    every model in the same run.

    The literal has no time zone: it is UTC. Warehouses with timezone-naive
    timestamps (Snowflake TIMESTAMP_NTZ, Postgres, Redshift) store it as is;
    those that apply the session time zone (Databricks) need a UTC session,
    which is the default.

    Usage in a model:
        {{ dbt_run_at() }} AS _dbt_run_at
#}
{% macro dbt_run_at() -%}
    CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") }}' AS {{ data_type('timestamp') }})
{%- endmacro %}
