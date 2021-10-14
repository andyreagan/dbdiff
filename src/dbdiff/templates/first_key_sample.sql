{% extends "first_key_base.sql" %}
{% block select %}
         SELECT {% if left %}y{% else %}x{% endif %}.{{ col }} AS {{ col }}
{% endblock %}
{% block orderby %}
                ORDER BY {% if left %}y{% else %}x{% endif %}.{{ col }}
{% endblock %}
{% block limit %}
                LIMIT {{ max_rows }}
{% endblock %}
