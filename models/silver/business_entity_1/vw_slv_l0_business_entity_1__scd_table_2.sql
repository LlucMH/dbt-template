-- History (SCD type 2) of the table_2 catalog, built from its snapshot.
-- l0: reads from a snapshot (history of the bronze layer).
-- Translates dbt's technical columns into business names, so that no consumer
-- depends on dbt_valid_from / dbt_valid_to / dbt_is_deleted.
--
-- Typical use: the description in force on the date of a fact
--   ... ON f.table_2_id = s.table_2_id
--       AND f.event_ts >= s.valid_from AND f.event_ts < s.valid_to
{{ config(materialized='view') }}

WITH snp_table_2 AS (
    SELECT
        field_a,
        field_b,
        dbt_valid_from,
        dbt_valid_to,
        dbt_is_deleted
    FROM {{ ref('snp_source_database_1__table_2') }}
),

final AS (
    SELECT
        field_a AS table_2_id,
        field_b AS table_2_desc,
        dbt_valid_from AS valid_from,
        dbt_valid_to AS valid_to,
        -- Same end date as dbt_valid_to_current in the snapshot.
        dbt_valid_to = CAST('9999-12-31' AS {{ data_type('timestamp') }}) AS is_current,
        -- dbt stores dbt_is_deleted as text ('True' / 'False').
        COALESCE(dbt_is_deleted = 'True', FALSE) AS is_deleted,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM snp_table_2
)

SELECT * FROM final
