SELECT COUNT(*)
  FROM {{ joined_schema }}.{{ joined_table }}
 WHERE {% if dialect == "vertica" -%}
 (x_{{ column }} <=> y_{{ column }}) IS FALSE
 {%- else -%}
 (x_{{ column }} IS DISTINCT FROM y_{{ column }})
 {%- endif %}
