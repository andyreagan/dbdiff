{% block select %}
{% endblock %}
{% block core %}
  FROM (
SELECT {% for col in columns -%}x.{{ col }} AS {{ col }}{% if not loop.last %},{% endif %}
       {% endfor -%}
  FROM {{ x_schema }}.{{ x_table }} x
INNER JOIN {{ y_schema }}.{{ y_table }} y
       ON {% for col in columns[:-1] %}x.{{ col }} {% if loop.first %}={% else %}<=>{% endif %} y.{{ col }}{% if not loop.last %} AND {% endif %}
       {% endfor -%}
GROUP BY {% for col in columns %}x.{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
       ) x
FULL OUTER JOIN (
SELECT {% for col in columns -%}y.{{ col }} AS {{ col }}{% if not loop.last %},{% endif %}
       {% endfor -%}
  FROM {{ y_schema }}.{{ y_table }} y
INNER JOIN {{ x_schema }}.{{ x_table }} x
       ON {% for col in columns[:-1] %}y.{{ col }} {% if loop.first %}={% else %}<=>{% endif %} x.{{ col }}{% if not loop.last %} AND {% endif %}
       {% endfor -%}
       GROUP BY {% for col in columns %}y.{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
       ) y
       ON {% for col in columns %}x.{{ col }} {% if loop.first %}={% else %}<=>{% endif %} y.{{ col }}{% if not loop.last %} AND {% endif %}
       {% endfor -%}
{% endblock %}
{% block where %}
       WHERE {% if left %}x{% else %}y{% endif %}.{{ columns[0] }} IS NULL
{% endblock %}
{% block groupby %}
{% endblock %}
{% block orderby %}
{% endblock %}
{% block limit %}
{% endblock %}
