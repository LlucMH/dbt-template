-- table_2 dimension enriched with its reporting group (seed).
-- l1: reads only from l0 models of its own layer.
-- Business rule visible in SQL: values not mapped in the seed are reported as 'NO GROUP'
-- and sorted last.
{{ config(materialized='view') }}

WITH dim_table_2 AS (
    SELECT
        table_2_id,
        table_2_desc
    FROM {{ ref('vw_slv_l0_business_entity_1__dim_table_2') }}
),

dim_table_2_group AS (
    SELECT
        table_2_id,
        group_id,
        group_desc,
        group_sort_order
    FROM {{ ref('vw_slv_l0_business_entity_1__dim_table_2_group') }}
),

final AS (
    SELECT
        d.table_2_id,
        d.table_2_desc,
        COALESCE(g.group_id, 'NO_GROUP') AS group_id,
        COALESCE(g.group_desc, 'NO GROUP') AS group_desc,
        COALESCE(g.group_sort_order, 999) AS group_sort_order,
        g.group_id IS NOT NULL AS is_group_mapped,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM dim_table_2 AS d
    LEFT JOIN dim_table_2_group AS g
        ON d.table_2_id = g.table_2_id
)

SELECT * FROM final
