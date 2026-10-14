-- Fact that unifies table_3 from source_database_1 and source_database_2.
-- Copies of the same table in different sources are unioned here with a
-- discriminator column (source_database), instead of carrying them separately
-- through every layer.
{{ config(materialized='view') }}

WITH table_3_source_database_1 AS (
    SELECT
        field_x,
        field_y,
        field_z
    FROM {{ ref('vw_brz_l0_source_database_1__table_3') }}
),

table_3_source_database_2 AS (
    SELECT
        field_x,
        field_y,
        field_z
    FROM {{ ref('vw_brz_l0_source_database_2__table_3') }}
),

unioned AS (
    SELECT
        'source_database_1' AS source_database,
        field_x,
        field_y,
        field_z
    FROM table_3_source_database_1

    UNION ALL

    SELECT
        'source_database_2' AS source_database,
        field_x,
        field_y,
        field_z
    FROM table_3_source_database_2
),

final AS (
    SELECT
        source_database,
        field_x AS table_3_id,
        field_y AS event_date,
        field_z AS amount,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM unioned
)

SELECT * FROM final
