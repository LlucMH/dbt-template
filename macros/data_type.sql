{#
    data_type

    Returns the physical data type of the active warehouse for a logical type,
    using the var `data_types` (dbt_project.yml).

    It is the project's single source of truth for types: SQL CASTs use this
    macro, and contract / seed data types in YAML files use
    var('data_types')[target.type].<type> (YAML files cannot call macros).
    This way the type a model returns and the type its contract declares come
    from the same definition on every warehouse.

    Logical types: string, integer, float, numeric, date, timestamp, boolean.

    Usage:
        CAST(col AS {{ data_type('string') }})
#}
{% macro data_type(name) -%}
    {%- set data_types = var('data_types') -%}
    {%- if target.type not in data_types -%}
        {{ exceptions.raise_compiler_error(
            "Adapter '" ~ target.type ~ "' is not in the var 'data_types' of dbt_project.yml. "
            ~ "Add its types to use it."
        ) }}
    {%- endif -%}
    {%- if name not in data_types[target.type] -%}
        {{ exceptions.raise_compiler_error(
            "Logical type '" ~ name ~ "' is not defined for '" ~ target.type ~ "' in the var 'data_types'."
        ) }}
    {%- endif -%}
    {{- data_types[target.type][name] -}}
{%- endmacro %}
