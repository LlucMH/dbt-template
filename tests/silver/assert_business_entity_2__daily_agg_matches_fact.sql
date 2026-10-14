-- Singular test: the daily aggregate neither loses nor duplicates records compared to the fact.
-- Returns rows (and therefore fails) if the totals do not match.
-- COALESCE in the SUM(): on an empty aggregate it would return NULL, and NULL != n is
-- never TRUE, so the test would pass without noticing that every record was lost.
WITH fact_total AS (
    SELECT COUNT(*) AS record_count
    FROM {{ ref('tb_slv_l1_business_entity_2__fact_table_3_partitioned') }}
),

agg_total AS (
    SELECT COALESCE(SUM(record_count), 0) AS record_count
    FROM {{ ref('vw_slv_l2_business_entity_2__agg_table_3_daily') }}
)

SELECT
    fact_total.record_count AS fact_record_count,
    agg_total.record_count AS agg_record_count
FROM fact_total
CROSS JOIN agg_total
WHERE fact_total.record_count != agg_total.record_count
