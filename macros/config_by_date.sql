{#
    table_config_by_date / incremental_config_by_date

    Return, as a dictionary for config(**...), the physical configuration of a
    table that is queried and reloaded by date, translated into what each
    warehouse offers. This is technical platform logic: the model SQL does not
    change between warehouses, only how the table is stored.

    | warehouse  | physical layout                       | incremental (rewrites whole days) |
    |------------|---------------------------------------|-----------------------------------|
    | bigquery   | daily partition_by + cluster_by       | insert_overwrite                  |
    | snowflake  | cluster_by                            | delete+insert by date             |
    | databricks | liquid_clustered_by                   | delete+insert by date             |
    | redshift   | sort + dist auto                      | delete+insert by date             |
    | postgres   | index on the date                     | delete+insert by date             |
    | duckdb     | none (local and CI engine)            | delete+insert by date             |

    delete+insert with unique_key = date deletes in the target every day returned
    by the query and inserts them again: the same effect as insert_overwrite.
    An adapter missing from the table uses the default branch (delete+insert, no
    physical layout): it works, but it should get its own branch.

    Usage:
        {{ config(materialized='table', **table_config_by_date('event_date', cluster_by=['source_database'])) }}
        {{ config(materialized='incremental', **incremental_config_by_date('event_date', cluster_by=['source_database'])) }}
#}
{% macro table_config_by_date(column, cluster_by=[]) -%}
    {%- set columns = [column] + cluster_by -%}
    {%- if target.type == 'bigquery' -%}
        {%- do return({
            'partition_by': {'field': column, 'data_type': 'date', 'granularity': 'day'},
            'cluster_by': cluster_by if cluster_by else none
        }) -%}
    {%- elif target.type == 'snowflake' -%}
        {%- do return({'cluster_by': columns}) -%}
    {%- elif target.type == 'databricks' -%}
        {%- do return({'liquid_clustered_by': columns}) -%}
    {%- elif target.type == 'redshift' -%}
        {%- do return({'sort': columns, 'dist': 'auto'}) -%}
    {%- elif target.type == 'postgres' -%}
        {%- do return({'indexes': [{'columns': [column]}]}) -%}
    {%- else -%}
        {%- do return({}) -%}
    {%- endif -%}
{%- endmacro %}

{% macro incremental_config_by_date(column, cluster_by=[]) -%}
    {%- set physical = table_config_by_date(column, cluster_by) -%}
    {%- if target.type == 'bigquery' -%}
        {%- do physical.update({'incremental_strategy': 'insert_overwrite'}) -%}
    {%- else -%}
        {%- do physical.update({'incremental_strategy': 'delete+insert', 'unique_key': column}) -%}
    {%- endif -%}
    {%- do return(physical) -%}
{%- endmacro %}
