SELECT joined.*
  FROM {{ joined_schema }}.{{ joined_table }} joined
 WHERE {% for column in columns -%}
 {%- if dialect == "vertica" -%}
 ((x_{{ column }} <=> y_{{ column }}) IS FALSE)
 {%- else -%}
 (x_{{ column }} IS DISTINCT FROM y_{{ column }})
 {%- endif -%}
 {%- if not loop.last %} OR {% endif %}{% endfor %}
