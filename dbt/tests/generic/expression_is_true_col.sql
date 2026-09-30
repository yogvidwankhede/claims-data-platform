{% test expression_is_true_col(model, column_name, expression) %}
select * from {{ model }} where not ({{ column_name }} {{ expression }})
{% endtest %}
