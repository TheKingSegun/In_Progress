/*
  Override dbt's default generate_schema_name macro.

  Default behaviour appends the custom schema to the target schema:
    e.g. "dbt_segun_core" in dev, "core" in prod

  This override:
    - In prod: uses the custom schema name as-is (clean schema names)
    - In dev/staging: prefixes with the developer's name to avoid collisions
*/

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}

        {%- if custom_schema_name is not none -%}
            {{ custom_schema_name | trim }}
        {%- else -%}
            {{ default_schema | trim }}
        {%- endif -%}

    {%- else -%}

        {%- if custom_schema_name is not none -%}
            {{ default_schema | trim }}_{{ custom_schema_name | trim }}
        {%- else -%}
            {{ default_schema | trim }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
