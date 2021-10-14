    SELECT x.*
{% if not use_temp_table %}INTO {{ schema_name }}.{{ table_name_dedup }}{% endif %}
      FROM {{ schema_name }}.{{ table_name }} x
INNER JOIN (
    SELECT {{ group_cols }},
           COUNT(*) AS dup_count
      FROM {{ schema_name }}.{{ table_name }}
  GROUP BY {{ group_cols }}
           ) y
           ON {{ join_cols }}
     WHERE y.dup_count = 1