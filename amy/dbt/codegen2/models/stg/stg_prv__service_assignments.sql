{{config(materialized='view')}}

with service_assignments as (
    select 
        ASSIGNMENT_ID,
        SERVICE_ID,
        CUSTOMER_ID,
        START_DATE,
        END_DATE,
        STATUS,
        PROVISIONING_STATUS,
        LAST_MODIFIED_DATE,
        NOTE,
        CREATED_AT,
        UPDATED_AT,
        case when STATUS = 'Active' then 0 else 1 end as Label
    from {{ source('raw', 'PRV_SERVICE_ASSIGNMENTS') }}
)

select *
from service_assignments