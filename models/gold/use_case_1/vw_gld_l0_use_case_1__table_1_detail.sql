-- table_1 detail with its table_2 description, for drill-down in BI.
-- Exception to the gold default (table): exposed as a view because it is rarely
-- queried and always filtered, so it reflects the latest data without duplicating
-- storage. Hence the vw_ prefix.
-- Business rule visible in SQL: only records with a valid relationship are published.
-- Every column is cast to its contract type (data_type() and var data_types), so the
-- type the model returns is the type its YAML declares on every warehouse.
{{ config(materialized='view') }}

WITH fact_table_1 AS (
    SELECT
        table_1_id,
        table_1_desc,
        table_2_id,
        is_valid_relationship
    FROM {{ ref('vw_slv_l0_business_entity_1__fact_table_1') }}
),

dim_table_2 AS (
    SELECT
        table_2_id,
        table_2_desc
    FROM {{ ref('vw_slv_l0_business_entity_1__dim_table_2') }}
),

final AS (
    SELECT
        CAST(f.table_1_id AS {{ data_type('string') }}) AS table_1_id,
        CAST(f.table_1_desc AS {{ data_type('string') }}) AS table_1_desc,
        CAST(f.table_2_id AS {{ data_type('string') }}) AS table_2_id,
        CAST(d.table_2_desc AS {{ data_type('string') }}) AS table_2_desc,
        {{ dbt_run_at() }} AS _dbt_run_at
    FROM fact_table_1 AS f
    INNER JOIN dim_table_2 AS d
        ON f.table_2_id = d.table_2_id
    WHERE f.is_valid_relationship
)

SELECT * FROM final
