{% extends "sub_keys_base.sql" %}
{% block select %}
SELECT {% if x %}x{% else %}y{% endif %}.{{ join_cols[-1] }} AS {{ join_cols[-1] }},
       COUNT(*)
{% endblock %}
{% block groupby %}
GROUP BY 1
{% endblock %}
{% block orderby %}
ORDER BY 2 DESC
{% endblock %}
{% block limit %}
LIMIT {{ max_rows_column }}
{% endblock %}
