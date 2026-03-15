SELECT COUNT(*)
  FROM {{ joined_schema }}.{{ joined_table }}
 WHERE {% for column in columns -%}
 {%- if dialect == "vertica" -%}
 ((x_{{ column }} <=> y_{{ column }}) IS FALSE)
 {%- elif dialect == "sqlite" -%}
 (x_{{ column }} IS NOT y_{{ column }})
 {%- else -%}
 (x_{{ column }} IS DISTINCT FROM y_{{ column }})
 {%- endif -%}
 {%- if not loop.last %} OR {% endif %}{% endfor %}
