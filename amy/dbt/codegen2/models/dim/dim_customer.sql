{{ config(materialized='table') }}

with source as (
    select
        md5(cast(stg_crm__customers.CUSTOMER_ID as text) || '-' || cast(stg_crm__accounts.ACCOUNT_ID as text)) as CustomerSK,
        stg_crm__customers.CUSTOMER_ID as CustomerID,
        stg_crm__accounts.ACCOUNT_ID as AccountID,
        TRIM(UPPER(stg_crm__customers.CITY)) as City,
        TRIM(UPPER(stg_crm__customers.STATE)) as State,
        TRIM(stg_crm__customers.ZIP_CODE) as ZipCode,
        TRIM(UPPER(stg_crm__customers.COUNTRY)) as Country,
        TRIM(UPPER(stg_crm__customers.CUSTOMER_SEGMENT)) as CustomerSegment,
        TRIM(UPPER(stg_crm__customers.CUSTOMER_TIER)) as CustomerTier,
        CAST(stg_crm__customers.START_DATE as DATE) as StartDate,
        TRIM(UPPER(stg_crm__customers.STATUS)) as Status,
        TRIM(UPPER(stg_crm__accounts.ACCOUNT_TYPE)) as AccountType,
        CAST(stg_crm__accounts.CREATION_DATE as DATE) as AccountCreationDate,
        TRIM(UPPER(stg_crm__accounts.STATUS)) as AccountStatus,
        TRIM(UPPER(stg_crm__accounts.CURRENCY_CODE)) as AccountCurrencyCode,
        CAST(stg_crm__accounts.ACCOUNT_BALANCE as DECIMAL(18,2)) as AccountBalance,
        current_timestamp as SCDStartDate,
        CAST('9999-12-31' as timestamp) as SCDEndDate,
        TRUE as SCDIsCurrent
    from {{ ref('stg_crm__customers') }} as stg_crm__customers
    inner join {{ ref('stg_crm__accounts') }} as stg_crm__accounts
        on stg_crm__customers.CUSTOMER_ID = stg_crm__accounts.CUSTOMER_ID
    where stg_crm__customers.CUSTOMER_ID is not null
      and stg_crm__accounts.ACCOUNT_ID is not null
)

select *
from source