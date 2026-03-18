{% macro sum_by_months(column_name, months) %}
    {% for m in months %}
    sum(case when transaction_month = {{ m }} then {{ column_name }} else 0 end) as {{ column_name }}_month_{{ m }}
    {%- if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}