-- Analysis: differences between the incremental fact and the full recomputation.
-- The incremental table only rewrites the last N days (var business_entity_2_reprocess_days).
-- Records that arrive later than that, or are changed in the source outside the window,
-- never reach it. This analysis compares it with the l0 view, which always recomputes
-- everything, and lists every difference.
-- Use: if it returns rows often, increase the reprocessing window; for a one-off fix,
-- dbt run -s tb_slv_l1_business_entity_2__fact_table_3_partitioned --full-refresh
--   - missing_in_incremental : in the source, not in the incremental table.
--   - only_in_incremental    : in the incremental table, no longer in the source.
--   - different_amount       : in both, with a different amount.
-- Note: reads the whole source; run it by hand, not on a schedule.
WITH full_recompute AS (
    SELECT
        source_database,
        table_3_id,
        event_date,
        amount
    FROM {{ ref('vw_slv_l0_business_entity_2__fact_table_3') }}
),

incremental AS (
    SELECT
        source_database,
        table_3_id,
        event_date,
        amount
    FROM {{ ref('tb_slv_l1_business_entity_2__fact_table_3_partitioned') }}
),

differences AS (
    SELECT
        COALESCE(f.source_database, i.source_database) AS source_database,
        COALESCE(f.table_3_id, i.table_3_id) AS table_3_id,
        COALESCE(f.event_date, i.event_date) AS event_date,
        f.amount AS source_amount,
        i.amount AS incremental_amount,
        CASE
            WHEN i.table_3_id IS NULL THEN 'missing_in_incremental'
            WHEN f.table_3_id IS NULL THEN 'only_in_incremental'
            ELSE 'different_amount'
        END AS difference_type
    FROM full_recompute AS f
    FULL OUTER JOIN incremental AS i
        ON
            f.source_database = i.source_database
            AND f.table_3_id = i.table_3_id
            AND f.event_date = i.event_date
    WHERE
        f.table_3_id IS NULL
        OR i.table_3_id IS NULL
        OR f.amount != i.amount
        OR (f.amount IS NULL AND i.amount IS NOT NULL)
        OR (f.amount IS NOT NULL AND i.amount IS NULL)
)

SELECT *
FROM differences
ORDER BY
    event_date DESC,
    source_database ASC,
    table_3_id ASC
