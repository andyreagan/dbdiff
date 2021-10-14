{% block select %}
{% endblock %}
{% block core %}
           FROM (
         SELECT {{ col }} FROM {{ x_schema }}.{{ x_table }} GROUP BY 1
                ) x
FULL OUTER JOIN (
         SELECT {{ col }} FROM {{ y_schema }}.{{ y_table }} GROUP BY 1
                ) y
                ON x.{{ col }} = y.{{ col }} -- first key shouldn't ever be null!
{% endblock %}
{% block where %}
                WHERE {% if left %}x{% else %}y{% endif %}.{{ col }} IS NULL
{% endblock %}
{% block orderby %}
{% endblock %}
{% block limit %}
{% endblock %}
