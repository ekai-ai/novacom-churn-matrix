-- Generic test for ensuring numeric columns have positive values
{% test positive_values(model, column_name) %}

with validation as (
    select
        *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} < 0
)

select * from validation

{% endtest %}
