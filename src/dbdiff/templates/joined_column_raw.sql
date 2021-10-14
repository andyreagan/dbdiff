    SELECT {{ join_cols|join(", ") }},
           x_{{ column }},
           y_{{ column }}
      FROM {{ joined_schema }}.{{ joined_table }}
     WHERE (x_{{ column }} <=> y_{{ column }}) IS FALSE
  ORDER BY {{ join_cols|join(", ") }}
