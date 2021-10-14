{% extends "sub_keys_base.sql" %}
{% block select %}
SELECT {% for col in columns -%}
       {% if left %}y{% else %}x{% endif %}.{{ col }} AS {{ col }}{% if not loop.last %},{% endif %}
       {% endfor -%}
{% endblock %}
{% block orderby %}
ORDER BY {% for col in columns %}{% if left %}y{% else %}x{% endif %}.{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
{% endblock %}
{% block limit %}
LIMIT {{ max_rows }}
{% endblock %}

