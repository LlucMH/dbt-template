-- BI report: table_1 records per table_2 and their share of the total.
-- l0: reads only from the silver layer.
-- Every column is cast to its contract type (data_type() and var data_types), so the
-- type the model returns is the type its YAML declares on every warehouse.
{{ config(materialized='table') }}

WITH agg_table_1_by_table_2 AS (
    SELECT
        table_2_id,
        table_2_desc,
        record_count
    FROM {{ ref('tb_slv_l1_business_entity_1__agg_table_1_by_table_2') }}
),

final AS (
    SELECT
        CAST(table_2_id AS {{ data_type('string') }}) AS table_2_id,
        CAST(table_2_desc AS {{ data_type('string') }}) AS table_2_desc,
        CAST(record_count AS {{ data_type('integer') }}) AS record_count,
        -- Cast to float before dividing: integer / integer truncates in Postgres and Redshift.
        CAST(
            CAST(record_count AS {{ data_type('float') }}) / NULLIF(SUM(record_count) OVER (), 0)
            AS {{ data_type('float') }}
        ) AS record_share,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM agg_table_1_by_table_2
)

SELECT * FROM final
