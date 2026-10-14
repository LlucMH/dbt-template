-- Fact built from table_1 of source_database_1.
-- l0: may join several bronze models (here, to validate the relationship with table_2).
{{ config(materialized='view') }}

WITH table_1 AS (
    SELECT
        field_1,
        field_2,
        field_3
    FROM {{ ref('vw_brz_l0_source_database_1__table_1') }}
),

table_2 AS (
    SELECT field_a
    FROM {{ ref('vw_brz_l0_source_database_1__table_2') }}
),

final AS (
    SELECT
        t1.field_1 AS table_1_id,
        t1.field_2 AS table_1_desc,
        t1.field_3 AS table_2_id,
        t2.field_a IS NOT NULL AS is_valid_relationship,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM table_1 AS t1
    LEFT JOIN table_2 AS t2
        ON t1.field_3 = t2.field_a
)

SELECT * FROM final
