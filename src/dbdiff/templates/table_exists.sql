{% if dialect == "vertica" -%}
select COUNT(*)
  from tables
 where table_schema = '{{ schema_name }}'
       and table_name = '{{ table_name }}'
{%- else -%}
select COUNT(*)
  from information_schema.tables
 where lower(table_schema) = lower('{{ schema_name }}')
       and lower(table_name) = lower('{{ table_name }}')
{%- endif %}
