{% test value_in_set(model, column_name, valid_values) %}

with validation as (
    select
        {{ column_name }} as value_field
    from {{ model }}
),

validation_errors as (
    select
        value_field
    from validation
    where value_field not in ({{ valid_values | join(", ") }})
    and value_field is not null
)

select *
from validation_errors

{% endtest %}
