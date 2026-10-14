-- Bronze model of a static catalog.
-- Normalizes text (spaces and upper case) and turns empty strings into NULL.
{{ config(materialized='view') }}

WITH src_table_2 AS (
    SELECT
        src_col_a,
        src_col_b
    FROM {{ source('source_database_2', 'table_2') }}
),

final AS (
    SELECT
        UPPER(TRIM(src_col_a)) AS field_a,
        NULLIF(TRIM(src_col_b), '') AS field_b,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM src_table_2
)

SELECT * FROM final
