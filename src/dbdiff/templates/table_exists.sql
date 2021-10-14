select COUNT(*)
  from tables
 where table_schema = '{{ schema_name }}'
       and table_name = '{{ table_name }}'