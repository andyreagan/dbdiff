  SELECT x_{{ column }},
         y_{{ column }},
         COUNT(*) AS ct
    FROM {{ joined_schema }}.{{ joined_table }}
   WHERE (x_{{ column }} <=> y_{{ column }}) IS FALSE
GROUP BY x_{{ column }}, y_{{ column }}
ORDER BY ct DESC