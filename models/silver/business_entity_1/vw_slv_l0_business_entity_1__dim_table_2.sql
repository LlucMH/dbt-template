-- Dimension built from the table_2 catalog of source_database_1.
-- l0: reads only from the bronze layer.
-- Business rule visible in SQL: empty descriptions are reported as 'NO DESCRIPTION'.
{{ config(materialized='view') }}

WITH table_2 AS (
    SELECT
        field_a,
        field_b
    FROM {{ ref('vw_brz_l0_source_database_1__table_2') }}
),

final AS (
    SELECT
        field_a AS table_2_id,
        COALESCE(field_b, 'NO DESCRIPTION') AS table_2_desc,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM table_2
)

SELECT * FROM final
