-- Daily aggregate of table_3 by source database.
-- l2: reads from the l1 fact organized by date, not from the l0 view, to avoid
-- recomputing the deduplication of the whole source on every query.
{{ config(materialized='view') }}

WITH fact_table_3 AS (
    SELECT
        event_date,
        source_database,
        amount
    FROM {{ ref('tb_slv_l1_business_entity_2__fact_table_3_partitioned') }}
),

final AS (
    SELECT
        event_date,
        source_database,
        COUNT(*) AS record_count,
        SUM(amount) AS total_amount,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM fact_table_3
    GROUP BY
        event_date,
        source_database
)

SELECT * FROM final
