{#
    validate_model_naming

    Validates the naming convention of every model in the project:

        <materialization>_<layer>_l<level>_<domain>__<table>

    materialization : tb (table, incremental) | vw (view) | ep (ephemeral)
    layer           : brz (bronze) | slv (silver) | gld (gold)
    level           : l0, l1, l2...  l0 reads from the previous layer (or from sources);
                      l<n> only reads from lower levels of its own layer.
    domain          : source_database (brz) | business_entity (slv) | use_case (gld)
                      Must match the model's subfolder.
    table           : in silver it must start with a table type:
                      fact_ | dim_ | scd_ | bridge_ | agg_

    It also checks:
      - that the materialization prefix matches the actual configuration;
      - that the layer matches the folder (bronze / silver / gold);
      - that no model depends on a later layer, or on an equal or higher
        level of its own layer;
      - that no model, seed or snapshot name is longer than var('max_name_length')
        characters (63 by default: the Postgres limit, the strictest of the
        supported warehouses; a longer name is truncated and breaks dbt).

    Usage:
      - Automatically on every run via on-run-start (dbt_project.yml).
      - In CI without running models:  dbt run-operation validate_model_naming
#}
{% macro validate_model_naming() %}
    {%- if execute -%}

        {%- set materialization_prefix = {
            'table': 'tb',
            'incremental': 'tb',
            'view': 'vw',
            'materialized_view': 'vw',
            'ephemeral': 'ep'
        } -%}
        {%- set layer_by_folder = {'bronze': 'brz', 'silver': 'slv', 'gold': 'gld'} -%}
        {%- set layer_rank = {'brz': 0, 'slv': 1, 'gld': 2} -%}
        {%- set silver_table_types = ['fact', 'dim', 'scd', 'bridge', 'agg'] -%}

        {%- set segment = '[a-z0-9]+(?:_[a-z0-9]+)*' -%}
        {%- set pattern = '^(tb|vw|ep)_(brz|slv|gld)_l([0-9]+)_(' ~ segment ~ ')__(' ~ segment ~ ')$' -%}

        {#- First pass: parse every name -#}
        {%- set parsed = {} -%}
        {%- set errors = [] -%}
        {%- set models = graph.nodes.values()
            | selectattr('resource_type', 'equalto', 'model')
            | selectattr('package_name', 'equalto', project_name)
            | list -%}

        {%- for node in models -%}
            {%- set match = modules.re.match(pattern, node.name) -%}
            {%- if not match -%}
                {%- do errors.append(node.name ~ ': does not follow the pattern <tb|vw|ep>_<brz|slv|gld>_l<n>_<domain>__<table>') -%}
            {%- else -%}
                {%- set mat, layer, level, domain, table = match.groups() -%}
                {%- do parsed.update({node.unique_id: {'layer': layer, 'level': level | int}}) -%}

                {%- set expected_mat = materialization_prefix.get(node.config.materialized) -%}
                {%- if expected_mat and expected_mat != mat -%}
                    {%- do errors.append(node.name ~ ': prefix ' ~ mat ~ '_ but materialized as ' ~ node.config.materialized ~ ' (expected ' ~ expected_mat ~ '_)') -%}
                {%- endif -%}

                {%- set folder_layer = layer_by_folder.get(node.fqn[1]) if node.fqn | length > 2 else none -%}
                {%- if folder_layer != layer -%}
                    {%- do errors.append(node.name ~ ': layer ' ~ layer ~ '_ but located in folder ' ~ (node.fqn[1] if node.fqn | length > 2 else 'models/')) -%}
                {%- endif -%}

                {%- if node.fqn | length > 3 and node.fqn[2] != domain -%}
                    {%- do errors.append(node.name ~ ': domain ' ~ domain ~ ' but located in subfolder ' ~ node.fqn[2]) -%}
                {%- endif -%}

                {%- if layer == 'slv' and table.split('_')[0] not in silver_table_types -%}
                    {%- do errors.append(node.name ~ ': in silver the table must start with ' ~ silver_table_types | join('_, ') ~ '_') -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}

        {#- Second pass: direction of dependencies -#}
        {%- for node in models if node.unique_id in parsed -%}
            {%- set me = parsed[node.unique_id] -%}
            {%- for dep_id in node.depends_on.nodes if dep_id in parsed -%}
                {%- set dep = parsed[dep_id] -%}
                {%- set dep_name = graph.nodes[dep_id].name -%}
                {%- if layer_rank[dep.layer] > layer_rank[me.layer] -%}
                    {%- do errors.append(node.name ~ ': depends on a later layer (' ~ dep_name ~ ')') -%}
                {%- elif dep.layer == me.layer and dep.level >= me.level -%}
                    {%- do errors.append(node.name ~ ': depends on an equal or higher level of its layer (' ~ dep_name ~ ')') -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}

        {#- Third pass: length of every name that creates a relation -#}
        {%- set max_len = var('max_name_length', 63) -%}
        {%- for node in graph.nodes.values()
            if node.resource_type in ['model', 'seed', 'snapshot'] and node.package_name == project_name -%}
            {%- if node.name | length > max_len -%}
                {%- do errors.append(node.name ~ ': ' ~ node.name | length ~ ' characters (maximum ' ~ max_len ~ ')') -%}
            {%- endif -%}
        {%- endfor -%}

        {%- if errors -%}
            {{ exceptions.raise_compiler_error(
                'Models that break the naming convention:\n  - ' ~ errors | join('\n  - ')
            ) }}
        {%- endif -%}

    {%- endif -%}
{% endmacro %}
