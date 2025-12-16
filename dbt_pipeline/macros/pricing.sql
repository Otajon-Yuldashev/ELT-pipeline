{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    ({{ extended_price}} * {{ discount_percentage }} * (-1))::numeric(16, {{ scale }})
{% endmacro %}
