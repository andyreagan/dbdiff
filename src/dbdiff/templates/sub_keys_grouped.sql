{% extends "sub_keys_base.sql" %}
{% block select %}
SELECT {% if left %}y{% else %}x{% endif %}.{{ columns[-1] }} AS {{ columns[-1] }},
       COUNT(*)
{% endblock %}
{% block groupby %}
GROUP BY 1
{% endblock %}
{% block orderby %}
ORDER BY 2 DESC
{% endblock %}
{% block limit %}
LIMIT {{ max_rows }}
{% endblock %}

