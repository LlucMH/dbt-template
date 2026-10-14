{#
    get_where_subquery

    Overrides the dbt macro that applies the `where` config of tests, so that
    warehouse-independent placeholders can be used. Macros cannot be called in
    the `where` of a test, so the placeholder is written there and replaced here:

        __test_window_start_date__  ->  today minus var('test_window_days') days, as DATE

    Usage in a models YAML:
        config:
          where: "event_date >= __test_window_start_date__"
#}
{% macro get_where_subquery(relation) -%}
    {%- set where = config.get('where') -%}
    {%- if where -%}
        {%- set window_start = 'CAST(' ~ dbt.dateadd('day', -var('test_window_days'), dbt.current_timestamp()) ~ ' AS ' ~ data_type('date') ~ ')' -%}
        {%- set where = where | replace('__test_window_start_date__', window_start) -%}
        {%- do return('(SELECT * FROM ' ~ relation ~ ' WHERE ' ~ where ~ ') AS dbt_subquery') -%}
    {%- else -%}
        {%- do return(relation) -%}
    {%- endif -%}
{%- endmacro %}
