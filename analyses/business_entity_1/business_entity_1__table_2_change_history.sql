-- Analysis: how much the table_2 catalog changes over time, from its SCD2 history.
-- Use: decide how often the snapshot must run (if values change several times a day,
-- a daily snapshot loses intermediate versions) and spot unexpected mass changes
-- (e.g. a source reload that rewrote every description).
-- One row per day with changes: new versions, deletions and values affected.
WITH scd_table_2 AS (
    SELECT
        table_2_id,
        valid_from,
        is_deleted
    FROM {{ ref('vw_slv_l0_business_entity_1__scd_table_2') }}
),

versions AS (
    SELECT
        table_2_id,
        CAST(valid_from AS {{ data_type('date') }}) AS change_date,
        is_deleted,
        ROW_NUMBER() OVER (PARTITION BY table_2_id ORDER BY valid_from) AS version_number
    FROM scd_table_2
),

changes_by_day AS (
    SELECT
        change_date,
        SUM(CASE WHEN version_number = 1 THEN 1 ELSE 0 END) AS first_versions,
        SUM(CASE WHEN version_number > 1 AND NOT is_deleted THEN 1 ELSE 0 END) AS updated_versions,
        SUM(CASE WHEN is_deleted THEN 1 ELSE 0 END) AS deletions,
        COUNT(DISTINCT table_2_id) AS values_affected
    FROM versions
    GROUP BY
        change_date
)

SELECT *
FROM changes_by_day
ORDER BY
    change_date DESC
