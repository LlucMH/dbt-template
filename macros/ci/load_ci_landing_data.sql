{#
    load_ci_landing_data

    Creates the landing tables of the sources with a few test rows, so that a
    full `dbt build` can run without real data (CI and local development on
    DuckDB, with no cost and no credentials):

        dbt run-operation load_ci_landing_data --target ci
        dbt build --target ci

    The data covers the cases the models handle: duplicated keys in table_3
    (the latest load wins), a catalog value with no group in the seed, and a
    record with an invalid relationship.

    Runs only on the `ci` target: it never creates tables in a real warehouse.
    It uses standard SQL and dbt macros, so it works on any adapter.
#}
{% macro load_ci_landing_data() %}
    {%- if target.name != 'ci' -%}
        {{ exceptions.raise_compiler_error("load_ci_landing_data only runs with --target ci (current target: " ~ target.name ~ ")") }}
    {%- endif -%}

    {%- set ts = "CAST('2026-01-01 00:00:00' AS " ~ data_type('timestamp') ~ ")" -%}
    {%- set ts_late = "CAST('2026-01-02 00:00:00' AS " ~ data_type('timestamp') ~ ")" -%}

    {%- set tables = {
        'table_1': "
            SELECT '1' AS src_col_1, ' Record 1 ' AS src_col_2, 'A01' AS src_col_3, " ~ ts ~ " AS _ingestion_timestamp
            UNION ALL SELECT '2', 'Record 2', 'B01', " ~ ts ~ "
            UNION ALL SELECT '3', 'Record 3', 'Z99', " ~ ts ~ "
            UNION ALL SELECT '4', 'Record 4', 'X00', " ~ ts,
        'table_2': "
            SELECT ' a01 ' AS src_col_a, 'Value A01' AS src_col_b, " ~ ts ~ " AS _ingestion_timestamp
            UNION ALL SELECT 'B01', '', " ~ ts ~ "
            UNION ALL SELECT 'Z99', 'Value with no group', " ~ ts,
        'table_3': "
            SELECT 'X1' AS src_col_x, '2026-01-01' AS src_col_y, '10.50' AS src_col_z, " ~ ts ~ " AS _ingestion_timestamp
            UNION ALL SELECT 'X1', '2026-01-01', '12.00', " ~ ts_late ~ "
            UNION ALL SELECT 'X2', '2026-01-01', '5', " ~ ts ~ "
            UNION ALL SELECT 'X1', '2026-01-02', '7.25', " ~ ts ~ "
            UNION ALL SELECT 'X3', '2026-02-01', '1', " ~ ts
    } -%}

    {%- for source_name in ['source_database_1', 'source_database_2'] -%}
        {%- for table_name, sql in tables.items() -%}
            {%- set relation = source(source_name, table_name).incorporate(type='table') -%}
            {%- do adapter.create_schema(relation) -%}
            {%- do adapter.drop_relation(relation) -%}
            {%- do run_query(dbt.create_table_as(False, relation, sql)) -%}
            {%- do log('CI: created ' ~ relation, info=True) -%}
        {%- endfor -%}
    {%- endfor -%}
{% endmacro %}
