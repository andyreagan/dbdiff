        SELECT {{ join_cols }},
               {{ column }}
          FROM {{ schema }}.{{ table }}
         WHERE {{ first_join_col }} IN (SELECT {{ first_join_col }} FROM {{ diff_schema }}.{{ diff_table }} WHERE column_name = '{{ column }}' GROUP BY {{ first_join_col }} ORDER BY {{ first_join_col }} {% if limit %}LIMIT {{ limit }}{% endif %})
      ORDER BY {{ join_cols }}