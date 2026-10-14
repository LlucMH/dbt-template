-- Analysis: gaps in the manual table_2 -> group mapping (seed).
-- dbt only compiles it (dbt compile); it creates nothing in the warehouse. The resulting
-- SQL lands in target/compiled/poc_dbt/analyses/business_entity_1/ to be run by hand.
-- Use: review it before updating the seed CSV, to know which rows to add or remove.
--   - no_group      : catalog values missing from the seed (reported as 'NO GROUP').
--   - orphan_group  : seed rows whose id no longer exists in the catalog.
WITH catalog AS (
    SELECT
        table_2_id,
        table_2_desc
    FROM {{ ref('vw_slv_l0_business_entity_1__dim_table_2') }}
),

mapping AS (
    SELECT
        table_2_id,
        group_id,
        group_desc
    FROM {{ ref('vw_slv_l0_business_entity_1__dim_table_2_group') }}
),

records AS (
    SELECT
        table_2_id,
        record_count
    FROM {{ ref('tb_slv_l1_business_entity_1__agg_table_1_by_table_2') }}
),

gaps AS (
    SELECT
        COALESCE(c.table_2_id, m.table_2_id) AS table_2_id,
        c.table_2_desc,
        m.group_id,
        m.group_desc,
        COALESCE(r.record_count, 0) AS record_count,
        CASE
            WHEN m.table_2_id IS NULL THEN 'no_group'
            WHEN c.table_2_id IS NULL THEN 'orphan_group'
        END AS gap_type
    FROM catalog AS c
    FULL OUTER JOIN mapping AS m
        ON c.table_2_id = m.table_2_id
    LEFT JOIN records AS r
        ON COALESCE(c.table_2_id, m.table_2_id) = r.table_2_id
    WHERE
        c.table_2_id IS NULL
        OR m.table_2_id IS NULL
)

SELECT *
FROM gaps
-- Gaps that leave the most records without a group in reports come first.
ORDER BY
    record_count DESC,
    table_2_id ASC
