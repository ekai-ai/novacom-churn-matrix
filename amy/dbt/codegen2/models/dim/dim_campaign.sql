{{ config(materialized='table') }}

with source as (
    select *
    from {{ ref('stg_mkt__campaigns') }}
    where CAMPAIGN_ID is not null
)

select
    {{ dbt_utils.surrogate_key(['CAMPAIGN_ID']) }} as CampaignSK,
    CAMPAIGN_ID as CampaignID,
    TRIM(NAME) as Name,
    UPPER(TRIM(CAMPAIGN_TYPE)) as CampaignType,
    TRIM(OBJECTIVE) as Objective,
    TRIM(TARGET_AUDIENCE) as TargetAudience,
    TRIM(CONVERSION_GOAL) as ConversionGoal,
    CAST(START_DATE as DATE) as StartDate,
    CAST(END_DATE as DATE) as EndDate,
    CAST(BUDGET as DECIMAL(18, 2)) as Budget,
    CURRENT_TIMESTAMP as SCDStartDate,
    CAST('9999-12-31' as TIMESTAMP) as SCDEndDate,
    TRUE as SCDIsCurrent
from source