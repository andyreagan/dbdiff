{% extends "sub_keys_base.sql" %}
{% block select %}
SELECT {% for col in join_cols -%}
       {% if x %}x{% else %}y{% endif %}.{{ col }} AS {{ col }}{% if not loop.last %},{% endif %}
       {% endfor -%}
{% endblock %}
{% block orderby %}
ORDER BY {% for col in join_cols %}{% if x %}x{% else %}y{% endif %}.{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
{% endblock %}
{% block limit %}
LIMIT {{ max_rows_column }}
{% endblock %}
