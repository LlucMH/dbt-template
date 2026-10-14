-- table_3 fact materialized as an incremental table organized by date.
-- l1: reads only from the l0 fact of its own layer.
-- Justified exception to the silver default (view), hence the tb_ prefix:
--   - table_3 is high-volume and is read by models that filter by date;
--     organizing the table by date lets the warehouse read only the requested days.
--   - recomputing the whole history on every run would be expensive and pointless:
--     only the last days change.
--
-- Every incremental run rewrites whole days returned by the query, so rerunning it
-- never duplicates rows. The strategy and physical layout of each warehouse
-- (partition in BigQuery, clustering in Snowflake and Databricks, sort key in
-- Redshift, index in Postgres) come from macros/config_by_date.sql.
--
-- Reprocessing window: the last N days before the latest loaded date, to pick up
-- late-arriving records. If a run fails or is skipped, the next one still catches
-- up from the latest loaded date.
--   Default N: var business_entity_2_reprocess_days (dbt_project.yml).
--   One-off longer reprocess: dbt run -s <model> --vars '{business_entity_2_reprocess_days: 30}'
--   Full reload:              dbt run -s <model> --full-refresh
--
-- on_schema_change: fail (inherited from dbt_project.yml): a column change requires
-- a deliberate --full-refresh instead of propagating silently.
{{
    config(
        materialized='incremental',
        **incremental_config_by_date('event_date', cluster_by=['source_database', 'table_3_id'])
    )
}}

-- The incremental filter goes in the import CTE: the earlier it applies, the less data is read.
WITH fact_table_3 AS (
    SELECT
        source_database,
        table_3_id,
        event_date,
        amount
    FROM {{ ref('vw_slv_l0_business_entity_2__fact_table_3') }}
    {% if is_incremental() %}
    -- From N days before the latest loaded date. COALESCE: if the table exists
    -- but is empty, MAX() returns NULL and nothing would ever be loaded without it.
    {% set latest_date = "COALESCE(MAX(event_date), CAST('1900-01-01' AS " ~ data_type('date') ~ "))" %}
    WHERE
        event_date >= (
            SELECT
                CAST(
                    {{ dbt.dateadd('day', -(var('business_entity_2_reprocess_days') | int), latest_date) }}
                    AS {{ data_type('date') }}
                )
            FROM {{ this }}
        )
    {% endif %}
),

final AS (
    SELECT
        source_database,
        table_3_id,
        event_date,
        amount,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM fact_table_3
)

SELECT * FROM final
