CREATE TABLE {{ joined_schema }}.{{ joined_table }} (
    {% for i, row in compare_cols.iterrows() %}
    {% if row.name in join_cols %}
    {{ row.name }} {{ row.x_dtype }}
    {% else %}
    x_{{ row.name }} {{ row.x_dtype }},
    y_{{ row.name }} {{ row.x_dtype }}
    {% endif %}
    {% if not loop.last %},{% endif %}
    {% endfor %}
)
ORDER BY {{ join_cols|join(", ") }}
;
