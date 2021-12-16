{% extends "first_key_base.sql" %}
{% block select %}
         SELECT {% if x %}x{% else %}y{% endif %}.{{ join_col }} AS {{ join_col }}
{% endblock %}
{% block orderby %}
                ORDER BY {% if x %}x{% else %}y{% endif %}.{{ join_col }}
{% endblock %}
{% block limit %}
                LIMIT {{ max_rows_column }}
{% endblock %}
