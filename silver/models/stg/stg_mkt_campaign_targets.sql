{{ config(materialized='table') }}

with raw_data as (
    select
        TARGET_ID,
        CAMPAIGN_ID,
        CUSTOMER_ID,
        ASSIGNED_DATE,
        STATUS,
        LAST_CONTACT_DATE,
        CHANNEL,
        RESPONSE_FLAG,
        CREATED_AT,
        UPDATED_AT
    from source('raw', 'MKT_CAMPAIGN_TARGETS')
)

select *
from raw_data;