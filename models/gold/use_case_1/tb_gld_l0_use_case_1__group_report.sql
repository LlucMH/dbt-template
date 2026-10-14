-- BI report: table_1 records per reporting group (seed) and their share of the total.
-- l0: reads only from the silver layer.
-- Every column is cast to its contract type (data_type() and var data_types), so the
-- type the model returns is the type its YAML declares on every warehouse.
{{ config(materialized='table') }}

WITH agg_table_1_by_table_2 AS (
    SELECT
        table_2_id,
        record_count
    FROM {{ ref('tb_slv_l1_business_entity_1__agg_table_1_by_table_2') }}
),

dim_table_2_with_group AS (
    SELECT
        table_2_id,
        group_id,
        group_desc,
        group_sort_order
    FROM {{ ref('vw_slv_l1_business_entity_1__dim_table_2_with_group') }}
),

records_by_group AS (
    SELECT
        d.group_id,
        d.group_desc,
        d.group_sort_order,
        SUM(a.record_count) AS record_count
    FROM agg_table_1_by_table_2 AS a
    INNER JOIN dim_table_2_with_group AS d
        ON a.table_2_id = d.table_2_id
    GROUP BY
        d.group_id,
        d.group_desc,
        d.group_sort_order
),

final AS (
    SELECT
        CAST(group_id AS {{ data_type('string') }}) AS group_id,
        CAST(group_desc AS {{ data_type('string') }}) AS group_desc,
        CAST(group_sort_order AS {{ data_type('integer') }}) AS group_sort_order,
        CAST(record_count AS {{ data_type('integer') }}) AS record_count,
        -- Cast to float before dividing: integer / integer truncates in Postgres and Redshift.
        CAST(
            CAST(record_count AS {{ data_type('float') }}) / NULLIF(SUM(record_count) OVER (), 0)
            AS {{ data_type('float') }}
        ) AS record_share,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM records_by_group
)

SELECT * FROM final
