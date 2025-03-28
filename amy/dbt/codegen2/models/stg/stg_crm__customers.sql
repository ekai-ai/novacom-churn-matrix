{{ config(materialized='table') }}

with source_data as (
    select
        CUSTOMER_ID,
        DATE_OF_BIRTH,
        CUSTOMER_SEGMENT,
        CUSTOMER_TIER,
        START_DATE,
        STATUS,
        NOTE,
        CREATED_AT,
        UPDATED_AT
    from {{ source('raw', 'CRM_CUSTOMERS') }}
    where START_DATE >= '2023-01-01'
)

select
    CUSTOMER_ID,
    DATE_OF_BIRTH,
    CUSTOMER_SEGMENT,
    CUSTOMER_TIER,
    START_DATE,
    STATUS,
    NOTE,
    CREATED_AT,
    UPDATED_AT,
    case when upper(STATUS) = 'ACTIVE' then 0 else 1 end as Label
from source_data;