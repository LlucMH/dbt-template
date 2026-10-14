-- Singular test: the monthly report adds up to exactly the same totals as the daily one.
-- Both feed consumers of use_case_2 (dashboard and CSV export); if they diverged,
-- each consumer would see a different figure for the same period.
-- Returns one row per mismatching measure (and therefore fails).
WITH daily_totals AS (
    SELECT
        COALESCE(SUM(total_amount), 0) AS total_amount,
        COALESCE(SUM(record_count), 0) AS record_count
    FROM {{ ref('tb_gld_l0_use_case_2__daily_amount') }}
),

monthly_totals AS (
    SELECT
        COALESCE(SUM(total_amount), 0) AS total_amount,
        COALESCE(SUM(record_count), 0) AS record_count
    FROM {{ ref('tb_gld_l1_use_case_2__monthly_amount') }}
)

SELECT
    'total_amount' AS measure,
    CAST(daily_totals.total_amount AS {{ data_type('string') }}) AS daily_value,
    CAST(monthly_totals.total_amount AS {{ data_type('string') }}) AS monthly_value
FROM daily_totals
CROSS JOIN monthly_totals
WHERE daily_totals.total_amount != monthly_totals.total_amount

UNION ALL

SELECT
    'record_count' AS measure,
    CAST(daily_totals.record_count AS {{ data_type('string') }}) AS daily_value,
    CAST(monthly_totals.record_count AS {{ data_type('string') }}) AS monthly_value
FROM daily_totals
CROSS JOIN monthly_totals
WHERE daily_totals.record_count != monthly_totals.record_count
