{{ config(materialized='table') }}

with source_data as (
    select
        -- Generate surrogate key using hash of the natural key (SERVICE_ID) as an example
        md5(CAST(SERVICE_ID as text))::integer as ServiceSK,
        SERVICE_ID as ServiceID,
        TRIM(SERVICE_NAME) as ServiceName,
        UPPER(TRIM(SERVICE_TYPE)) as ServiceType,
        TRIM(DESCRIPTION) as Description,
        CAST(MONTHLY_COST AS DECIMAL(18,2)) as MonthlyCost,
        CAST(ACTIVE_FLAG AS BOOLEAN) as ActiveFlag,
        TRIM(PROVISIONING_TEAM) as ProvisioningTeam,
        CAST(CREATED_DATE AS DATE) as CreatedDate,
        current_timestamp as SCDStartDate,
        CAST('9999-12-31' as TIMESTAMP) as SCDEndDate,
        TRUE as SCDIsCurrent
    from {{ ref('stg_prv__services') }}
    where SERVICE_ID IS NOT NULL
)

select *
from source_data;