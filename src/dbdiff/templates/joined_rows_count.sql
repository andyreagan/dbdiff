SELECT COUNT(*)
  FROM {{ joined_schema }}.{{ joined_table }}
 WHERE {% for column in columns %}((x_{{ column }} <=> y_{{ column }}) IS FALSE){% if not loop.last %} OR {% endif %}{% endfor %}