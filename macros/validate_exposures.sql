{#
    validate_exposures

    Checks the orchestration contract of every exposure, which the Airflow DAGs
    (dags/poc_dbt_dags.py) rely on. A mistake here would otherwise leave a consumer
    without a DAG, or alert nobody, without any error:

      - exactly one schedule tag: daily | weekly | monthly | custom;
      - custom exposures declare meta.cron (5 fields, UTC); the others do not
        (their time comes from the shared schedule);
      - meta.ready_by in "HH:MM" format (UTC);
      - meta.notify (team identifier) and meta.criticality (low | medium | high);
      - no meta.schedule: the schedule is the tag, declared once.

    Usage (CI, without running models):
        dbt run-operation validate_exposures
#}
{% macro validate_exposures() %}
    {%- if execute -%}

        {%- set schedules = ['daily', 'weekly', 'monthly', 'custom'] -%}
        {%- set criticalities = ['low', 'medium', 'high'] -%}
        {%- set errors = [] -%}

        {%- for exposure in graph.exposures.values() if exposure.package_name == project_name -%}
            {%- set meta = exposure.config.meta or exposure.meta or {} -%}
            {%- set schedule_tags = exposure.tags | select('in', schedules) | list -%}

            {%- if schedule_tags | length != 1 -%}
                {%- do errors.append(exposure.name ~ ': needs exactly one schedule tag (' ~ schedules | join(', ') ~ '), has ' ~ (schedule_tags | join(', ') or 'none')) -%}
            {%- elif schedule_tags[0] == 'custom' -%}
                {%- if not meta.get('cron') or (meta.get('cron') | string).split() | length != 5 -%}
                    {%- do errors.append(exposure.name ~ ': custom schedule without a valid meta.cron (5 fields, UTC)') -%}
                {%- endif -%}
            {%- elif meta.get('cron') -%}
                {%- do errors.append(exposure.name ~ ': meta.cron is only allowed with the custom tag (' ~ schedule_tags[0] ~ ' uses its shared schedule)') -%}
            {%- endif -%}

            {%- if not modules.re.match('^([01][0-9]|2[0-3]):[0-5][0-9]$', meta.get('ready_by', '') | string) -%}
                {%- do errors.append(exposure.name ~ ': meta.ready_by must be "HH:MM" (UTC), got "' ~ meta.get('ready_by', '') ~ '"') -%}
            {%- endif -%}

            {%- if not meta.get('notify') -%}
                {%- do errors.append(exposure.name ~ ': meta.notify (team identifier) is required') -%}
            {%- endif -%}

            {%- if meta.get('criticality') not in criticalities -%}
                {%- do errors.append(exposure.name ~ ': meta.criticality must be one of ' ~ criticalities | join(', ')) -%}
            {%- endif -%}

            {%- if 'schedule' in meta -%}
                {%- do errors.append(exposure.name ~ ': remove meta.schedule, the schedule is the tag') -%}
            {%- endif -%}
        {%- endfor -%}

        {%- if errors -%}
            {{ exceptions.raise_compiler_error(
                'Exposures that break the orchestration contract:\n  - ' ~ errors | join('\n  - ')
            ) }}
        {%- else -%}
            {% do log('validate_exposures: every exposure follows the orchestration contract.', info=True) %}
        {%- endif -%}

    {%- endif -%}
{% endmacro %}
