SELECT COUNT(*)
  FROM {{ joined_schema }}.{{ joined_table }}
 WHERE (x_{{ column }} <=> y_{{ column }}) IS FALSE