{% if dialect == "vertica" -%}
CREATE LOCAL TEMP TABLE {{ table_name }} ON COMMIT PRESERVE ROWS AS ({{ query }})
{%- else -%}
CREATE TEMP TABLE {{ table_name }} AS ({{ query }})
{%- endif %}
