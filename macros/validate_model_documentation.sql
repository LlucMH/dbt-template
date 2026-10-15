{#
    validate_model_documentation

    Checks the documentation and testing rules of the template, which would
    otherwise only live in comments:

      - every model has a description;
      - every column declared in its YAML has a description;
      - every model declares the _dbt_run_at column;
      - every model has a primary key test: unique on a column, or
        unique_combination_of_columns at model level;
      - every Python model has the `python` tag (dbt build --exclude tag:python...).

    Placeholder descriptions ("...") count as descriptions: the template ships
    them on purpose. Replace them when the project uses real tables.

    Usage (CI, without running models):
        dbt run-operation validate_model_documentation
#}
{% macro validate_model_documentation() %}
    {%- if execute -%}

        {%- set primary_key_tests = ['unique', 'unique_combination_of_columns'] -%}
        {%- set models_with_primary_key = [] -%}
        {%- for test in graph.nodes.values() if test.resource_type == 'test' and test.test_metadata -%}
            {%- if test.test_metadata.name in primary_key_tests and test.attached_node -%}
                {%- do models_with_primary_key.append(test.attached_node) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- set errors = [] -%}
        {%- for node in graph.nodes.values()
            if node.resource_type == 'model' and node.package_name == project_name -%}

            {%- if not node.description | trim -%}
                {%- do errors.append(node.name ~ ': the model has no description') -%}
            {%- endif -%}

            {%- for column in node.columns.values() if not column.description | trim -%}
                {%- do errors.append(node.name ~ ': column ' ~ column.name ~ ' has no description') -%}
            {%- endfor -%}

            {%- if '_dbt_run_at' not in node.columns -%}
                {%- do errors.append(node.name ~ ': does not declare the _dbt_run_at column in its YAML') -%}
            {%- endif -%}

            {%- if node.unique_id not in models_with_primary_key -%}
                {%- do errors.append(node.name ~ ': has no primary key test (unique or unique_combination_of_columns)') -%}
            {%- endif -%}

            {%- if node.language == 'python' and 'python' not in node.tags -%}
                {%- do errors.append(node.name ~ ': Python model without the python tag') -%}
            {%- endif -%}

        {%- endfor -%}

        {%- if errors -%}
            {{ exceptions.raise_compiler_error(
                'Models that break the documentation rules:\n  - ' ~ errors | join('\n  - ')
            ) }}
        {%- else -%}
            {% do log('validate_model_documentation: all models follow the documentation rules.', info=True) %}
        {%- endif -%}

    {%- endif -%}
{% endmacro %}
