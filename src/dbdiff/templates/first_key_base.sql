{% block select %}
{% endblock %}
{% block core %}
           FROM (
         SELECT {{ join_col }} FROM {{ x_schema }}.{{ x_table }} GROUP BY 1
                ) x
FULL OUTER JOIN (
         SELECT {{ join_col }} FROM {{ y_schema }}.{{ y_table }} GROUP BY 1
                ) y
                ON x.{{ join_col }} = y.{{ join_col }}
{% endblock %}
{% block where %}
                WHERE {% if x %}y{% else %}x{% endif %}.{{ join_col }} IS NULL
{% endblock %}
{% block orderby %}
{% endblock %}
{% block limit %}
{% endblock %}
