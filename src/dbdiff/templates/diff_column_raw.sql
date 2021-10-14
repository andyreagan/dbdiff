    SELECT {% for col in join_cols %}joined.{{ col }},
           {% endfor %}joined.x_{{ column }},
           joined.y_{{ column }}
      FROM {{ joined_schema }}.{{ joined_table }} joined
INNER JOIN (
    SELECT {{ join_cols|join(", ") }}
      FROM {{ diff_schema }}.{{ diff_table }}
     WHERE column_name = '{{ column }}'
  GROUP BY {{ join_cols|join(", ") }}
           ) diff
           ON {{ join_cols_join }}