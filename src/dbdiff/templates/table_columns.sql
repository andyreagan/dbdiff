select column_name, data_type
  from columns
 where lower(table_schema) = lower('{{ schema_name }}')
       and lower(table_name) = lower('{{ table_name }}')
 order by ordinal_position