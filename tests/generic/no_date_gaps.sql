{#
    no_date_gaps

    Custom generic test: fails for every date that does not directly follow the
    previous one, i.e. for every gap in a series that should be continuous
    (one row per day, per month...). A gap usually means a load that failed or
    a period with no source data that nobody noticed.

    dbt-checks (packages.yml) covers row-level checks; this one compares each row
    with the previous one, which is the kind of check worth writing in the project.

    Generic tests live in tests/generic/ and are applied from the YAML like any
    built-in test:

        columns:
          - name: month_start_date
            data_tests:
              - no_date_gaps:
                  arguments:
                    datepart: month

    Arguments:
        datepart  grain of the series: day (default), week, month, year...

    Warehouse-agnostic: window function LAG() and dbt.dateadd().
#}
{% test no_date_gaps(model, column_name, datepart='day') %}

    WITH dates AS (
        SELECT DISTINCT {{ column_name }} AS series_date
        FROM {{ model }}
        WHERE {{ column_name }} IS NOT NULL
    ),

    with_previous AS (
        SELECT
            series_date,
            LAG(series_date) OVER (ORDER BY series_date) AS previous_date
        FROM dates
    )

    SELECT
        previous_date,
        series_date
    FROM with_previous
    WHERE
        previous_date IS NOT NULL
        AND CAST({{ dbt.dateadd(datepart, 1, 'previous_date') }} AS {{ data_type('date') }}) < series_date

{% endtest %}
