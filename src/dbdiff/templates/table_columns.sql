{% if dialect == "vertica" -%}
select column_name, data_type
  from columns
 where lower(table_schema) = lower('{{ schema_name }}')
       and lower(table_name) = lower('{{ table_name }}')
 order by ordinal_position
{%- else -%}
select column_name, data_type
  from information_schema.columns
 where lower(table_schema) = lower('{{ schema_name }}')
       and lower(table_name) = lower('{{ table_name }}')
 order by ordinal_position
{%- endif %}
