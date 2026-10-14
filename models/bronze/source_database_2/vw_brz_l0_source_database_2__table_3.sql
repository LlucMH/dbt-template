-- Bronze model of a high-volume table with a composite key (field_x, field_y).
-- dbt.safe_cast: a malformed source value becomes NULL instead of breaking the view
-- (SAFE_CAST / TRY_CAST depending on the warehouse; a plain CAST in Postgres and Redshift).
-- Landing may contain the same key in several loads: the latest one is kept.
{{ config(materialized='view') }}

WITH src_table_3 AS (
    SELECT
        src_col_x,
        src_col_y,
        src_col_z,
        _ingestion_timestamp
    FROM {{ source('source_database_2', 'table_3') }}
),

numbered_loads AS (
    SELECT
        src_col_x,
        src_col_y,
        src_col_z,
        ROW_NUMBER() OVER (
            PARTITION BY src_col_x, src_col_y
            ORDER BY _ingestion_timestamp DESC
        ) AS load_number
    FROM src_table_3
),

latest_load AS (
    SELECT
        src_col_x,
        src_col_y,
        src_col_z
    FROM numbered_loads
    WHERE load_number = 1
),

final AS (
    SELECT
        CAST(src_col_x AS {{ data_type('string') }}) AS field_x,
        {{ dbt.safe_cast('src_col_y', data_type('date')) }} AS field_y,
        {{ dbt.safe_cast('src_col_z', data_type('numeric')) }} AS field_z,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM latest_load
)

SELECT * FROM final
