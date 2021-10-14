INSERT INTO {{ joined_schema }}.{{ joined_table }} (
    {%- for i, row in compare_cols.iterrows() %}
    {% if row.name in join_cols -%}
    {{- row.name -}}
    {% else -%}
    x_{{- row.name }},
    y_{{ row.name -}}
    {%- endif -%}
    {%- if not loop.last %},{% endif -%}
    {%- endfor %}
)
(
     SELECT {% for i, row in compare_cols.iterrows() -%}
            {%- if row.name in join_cols -%}
            COALESCE(x.{{ row.name }}, y.{{ row.name }}) AS {{ row.name -}}
            {%- else -%}
            x.{{ row.name }}::{{ row.x_dtype }} AS x_{{ row.name }},
            y.{{ row.name }}::{{ row.x_dtype }} AS y_{{ row.name -}}
            {%- endif -%}
            {%- if not loop.last %},{% endif -%}
            {% endfor %}
       FROM {{ x_schema }}.{{ x_table }} AS x
 INNER JOIN {{ y_schema }}.{{ y_table }} AS y
            ON {% for col in join_cols %}x.{{ col }} <=> y.{{ col }}{% if not loop.last %} AND {% endif %}
            {% endfor %}
)