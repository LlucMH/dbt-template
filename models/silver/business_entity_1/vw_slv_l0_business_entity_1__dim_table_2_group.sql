-- Dimension of reporting groups built from the table_2 -> group mapping seed.
-- l0: reads from a seed (reference data, at the same level as the bronze layer).
-- Normalizes the key like vw_brz_l0_source_database_1__table_2 so that the join is reliable.
{{ config(materialized='view') }}

WITH table_2_group_map AS (
    SELECT
        table_2_id,
        group_id,
        group_desc,
        group_sort_order
    FROM {{ ref('business_entity_1__table_2_group_map') }}
),

final AS (
    SELECT
        UPPER(TRIM(table_2_id)) AS table_2_id,
        UPPER(TRIM(group_id)) AS group_id,
        TRIM(group_desc) AS group_desc,
        group_sort_order,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM table_2_group_map
)

SELECT * FROM final
