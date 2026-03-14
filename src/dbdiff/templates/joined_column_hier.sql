        SELECT {{ join_cols|join(", ") }},
               {{ column }}
          FROM {{ schema }}.{{ table }}
         WHERE {{ join_cols[0] }} IN (
             SELECT {{ join_cols[0] }}
             FROM {{ joined_schema }}.{{ joined_table }}
             WHERE {% if dialect == "vertica" -%}
             (x_{{ column }} <=> y_{{ column }}) IS FALSE
             {%- else -%}
             (x_{{ column }} IS DISTINCT FROM y_{{ column }})
             {%- endif %}
             ORDER BY {{ join_cols|join(", ") }}
             {% if limit %}LIMIT {{ limit }}{% endif %}
         )
      ORDER BY {{ join_cols|join(", ") }}
