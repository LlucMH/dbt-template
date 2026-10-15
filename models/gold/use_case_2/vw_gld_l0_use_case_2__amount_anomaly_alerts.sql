-- Amount anomalies of the latest loaded day, to be sent as an alert.
-- l0: reads only from the silver layer (Python anomaly detection model).
-- dbt only prepares WHAT to send; the Airflow DAG (dags/) sends it after dbt build,
-- and only when this view returns rows. An empty view means nothing to report.
-- View: always reflects the latest anomalies, nothing to store.
-- Disabled on Postgres and Redshift, like the Python model it reads (see the YAML).
-- Every column is cast to its contract type (data_type() and var data_types), so the
-- type the model returns is the type its YAML declares on every warehouse.
{{ config(materialized='view') }}

WITH daily_anomalies AS (
    SELECT
        event_date,
        source_database,
        total_amount,
        baseline_mean,
        z_score,
        is_anomaly
    FROM {{ ref('tb_slv_l3_business_entity_2__agg_table_3_daily_anomalies') }}
),

latest_day AS (
    SELECT MAX(event_date) AS event_date
    FROM daily_anomalies
),

final AS (
    SELECT
        CAST(a.event_date AS {{ data_type('date') }}) AS event_date,
        CAST(a.source_database AS {{ data_type('string') }}) AS source_database,
        CAST(a.total_amount AS {{ data_type('float') }}) AS total_amount,
        CAST(a.baseline_mean AS {{ data_type('float') }}) AS baseline_mean,
        CAST(a.z_score AS {{ data_type('float') }}) AS z_score,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM daily_anomalies AS a
    INNER JOIN latest_day AS l
        ON a.event_date = l.event_date
    WHERE a.is_anomaly
)

SELECT * FROM final
