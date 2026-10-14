{#
    not_in_future

    Custom generic test: fails for every row whose date is later than today.
    A date in the future usually means a parsing error in the source (day and
    month swapped, wrong century...).

    Generic tests live in tests/generic/ and are applied from the YAML like any
    built-in test:

        columns:
          - name: event_date
            data_tests:
              - not_in_future

    Warehouse-agnostic: dbt.current_timestamp() and data_type('date').
#}
{% test not_in_future(model, column_name) %}

    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} > CAST({{ dbt.current_timestamp() }} AS {{ data_type('date') }})

{% endtest %}
