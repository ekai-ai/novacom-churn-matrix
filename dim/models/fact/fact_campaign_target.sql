{{ config(materialized='table') }}

with source_data as (
    select
        TARGET_ID,
        CAMPAIGN_ID,
        CUSTOMER_ID,
        CAST(ASSIGNED_DATE as date) as ASSIGNED_DATE_KEY,
        STATUS,
        case when LAST_CONTACT_DATE is not null then CAST(LAST_CONTACT_DATE as date) else null end as LAST_CONTACT_DATE_KEY,
        CHANNEL,
        RESPONSE_FLAG
    from {{ ref('stg_mkt_campaign_targets') }}
    where TARGET_ID IS NOT NULL
      and CAMPAIGN_ID IS NOT NULL
      and CUSTOMER_ID IS NOT NULL
      and ASSIGNED_DATE IS NOT NULL
      and LAST_CONTACT_DATE IS NULL
      and STATUS IN ('Targeted', 'Contacted', 'Converted', 'Unsubscribed')
      and CHANNEL IN ('Email', 'SMS', 'Phone', 'Direct Mail')
      and RESPONSE_FLAG IS NULL
)

select *
from source_data;
