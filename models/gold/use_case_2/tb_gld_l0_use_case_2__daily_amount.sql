-- Daily amount of table_3 with one column per source database, exported to a file.
-- l0: reads only from the silver layer.
-- Organized by date (partition, clustering or sort key depending on the warehouse, see
-- macros/config_by_date.sql): consumers always filter by date range.
-- Every column is cast to its contract type (data_type() and var data_types), so the
-- type the model returns is the type its YAML declares on every warehouse.
{{ config(materialized='table', **table_config_by_date('event_date')) }}

WITH agg_table_3_daily AS (
    SELECT
        event_date,
        source_database,
        record_count,
        total_amount
    FROM {{ ref('vw_slv_l2_business_entity_2__agg_table_3_daily') }}
),

final AS (
    SELECT
        CAST(event_date AS {{ data_type('date') }}) AS event_date,
        CAST(
            SUM(CASE WHEN source_database = 'source_database_1' THEN total_amount END)
            AS {{ data_type('numeric') }}
        ) AS amount_source_database_1,
        CAST(
            SUM(CASE WHEN source_database = 'source_database_2' THEN total_amount END)
            AS {{ data_type('numeric') }}
        ) AS amount_source_database_2,
        CAST(SUM(total_amount) AS {{ data_type('numeric') }}) AS total_amount,
        CAST(SUM(record_count) AS {{ data_type('integer') }}) AS record_count,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM agg_table_3_daily
    GROUP BY
        event_date
)

SELECT * FROM final
