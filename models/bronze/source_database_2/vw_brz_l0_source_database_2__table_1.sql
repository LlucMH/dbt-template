-- Bronze model: renaming, casting and light cleaning of a source table.
-- No joins between entities, no aggregations, no business logic.
-- Explicit columns: never SELECT * from a source.
-- Types through data_type(): the active warehouse decides the physical type (var data_types).
{{ config(materialized='view') }}

WITH src_table_1 AS (
    SELECT
        src_col_1,
        src_col_2,
        src_col_3,
        _ingestion_timestamp
    FROM {{ source('source_database_2', 'table_1') }}
),

final AS (
    SELECT
        CAST(src_col_1 AS {{ data_type('string') }}) AS field_1,
        TRIM(src_col_2) AS field_2,
        CAST(src_col_3 AS {{ data_type('string') }}) AS field_3,
        CAST(_ingestion_timestamp AS {{ data_type('timestamp') }}) AS updated_at,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM src_table_1
)

SELECT * FROM final
