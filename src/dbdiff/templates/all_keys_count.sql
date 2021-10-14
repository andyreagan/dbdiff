     SELECT COUNT(*)
       FROM {{ x_schema }}.{{ x_table }} x
 FULL OUTER JOIN {{ y_schema }}.{{ y_table }} y
           ON {% for col in join_cols %}x.{{ col }} <=> y.{{ col }}{% if not loop.last %} AND {% endif %}
           {% endfor -%}
           WHERE {% if left %}x{% else %}y{% endif %}.{{ join_cols[0] }} IS NULL