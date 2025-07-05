{% macro describe_table(table_name) %}
    {% set query %}
        DESCRIBE TABLE {{ table_name }}
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% for row in results %}
            {{ log(row, info=true) }}
        {% endfor %}
    {% endif %}
{% endmacro %} 