{#- Fails on every row where the expression is false. -#}
{% test expression_is_true(model, expression) %}
select * from {{ model }} where not ({{ expression }})
{% endtest %}
