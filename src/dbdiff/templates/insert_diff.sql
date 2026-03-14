   INSERT INTO {{ diff_schema }}.{{ diff_table }} ( {{ join_cols|join(", ") }}, column_name )
        SELECT {{ join_cols|join(", ") }},
               '{{ column }}' as column_name
          FROM (
        SELECT {{ join_cols|join(", ") }},
               {% if dialect == "vertica" -%}
               x_{{ column }} <=> y_{{ column }} AS {{ column }}_eq
               {%- else -%}
               (x_{{ column }} IS NOT DISTINCT FROM y_{{ column }}) AS {{ column }}_eq
               {%- endif %}
          FROM {{ joined_schema }}.{{ joined_table }}
               ) AS t1
         WHERE t1.{{ column }}_eq IS FALSE;
