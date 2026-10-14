-- Aggregate of table_1 by table_2.
-- l1: reads only from l0 models of its own layer.
-- Materialized as a table (hence the tb_ prefix) because several gold models read it
-- and recomputing the aggregation on every query would be expensive.
{{ config(materialized='table') }}

WITH fact_table_1 AS (
    SELECT table_2_id
    FROM {{ ref('vw_slv_l0_business_entity_1__fact_table_1') }}
),

dim_table_2 AS (
    SELECT
        table_2_id,
        table_2_desc
    FROM {{ ref('vw_slv_l0_business_entity_1__dim_table_2') }}
),

final AS (
    SELECT
        f.table_2_id,
        d.table_2_desc,
        COUNT(*) AS record_count,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM fact_table_1 AS f
    INNER JOIN dim_table_2 AS d
        ON f.table_2_id = d.table_2_id
    GROUP BY
        f.table_2_id,
        d.table_2_desc
)

SELECT * FROM final
