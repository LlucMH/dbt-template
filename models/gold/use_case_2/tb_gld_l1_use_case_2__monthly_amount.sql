-- Monthly amount of table_3, for the dashboard summary.
-- l1: reads only from the l0 model of its own layer, so daily and monthly always match.
-- Every column is cast to its contract type (data_type() and var data_types), so the
-- type the model returns is the type its YAML declares on every warehouse.
{{ config(materialized='table') }}

WITH daily_amount AS (
    SELECT
        event_date,
        amount_source_database_1,
        amount_source_database_2,
        total_amount,
        record_count
    FROM {{ ref('tb_gld_l0_use_case_2__daily_amount') }}
),

daily_amount_with_month AS (
    SELECT
        -- date_trunc returns a timestamp on several warehouses: cast back to DATE.
        CAST({{ dbt.date_trunc('month', 'event_date') }} AS {{ data_type('date') }}) AS month_start_date,
        amount_source_database_1,
        amount_source_database_2,
        total_amount,
        record_count
    FROM daily_amount
),

final AS (
    SELECT
        month_start_date,
        CAST(SUM(amount_source_database_1) AS {{ data_type('numeric') }}) AS amount_source_database_1,
        CAST(SUM(amount_source_database_2) AS {{ data_type('numeric') }}) AS amount_source_database_2,
        CAST(SUM(total_amount) AS {{ data_type('numeric') }}) AS total_amount,
        CAST(SUM(record_count) AS {{ data_type('integer') }}) AS record_count,
        CAST(COUNT(*) AS {{ data_type('integer') }}) AS days_with_data,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM daily_amount_with_month
    GROUP BY
        month_start_date
)

SELECT * FROM final
