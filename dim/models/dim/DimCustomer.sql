{{ config(
    materialized='table',
    unique_key='CUSTOMER_ID',
    sort='START_DATE',
    dist='CUSTOMER_ID',
    schema='dimensions',
    alias='DimCustomer',
    tags=['dimensions', 'customer'],
    pre_hook=[
      "SET query_group = 'dbt_users'",
      "GRANT USAGE ON ALL SCHEMAS IN DATABASE {{ target.database }} TO GROUP dbt_users"
    ],
    post_hook=[
      "GRANT SELECT ON TABLE {{ this }} TO GROUP biusers"
    ]
) }}

WITH crm_customers_accounts_contacts AS (
    SELECT
        c.CUSTOMER_ID,
        c.FIRST_NAME,
        c.MIDDLE_NAME,
        c.LAST_NAME,
        c.EMAIL,
        c.PHONE,
        c.DATE_OF_BIRTH,
        c.ADDRESS_LINE1,
        c.ADDRESS_LINE2,
        c.CITY,
        c.STATE,
        c.ZIP_CODE,
        c.COUNTRY,
        c.CUSTOMER_SEGMENT,
        c.CUSTOMER_TIER,
        c.START_DATE,
        c.STATUS,
        a.ACCOUNT_TYPE,
        a.CREATION_DATE,
        a.ACCOUNT_BALANCE,
        a.BILLING_ADDRESS,
        a.SHIPPING_ADDRESS,
        ct.CONTACT_TYPE,
        ct.CONTACT_VALUE,
        ct.IS_PRIMARY,
        ct.CONTACT_LABEL
    FROM
        {{ ref('stg_crm_customers') }} c
    LEFT JOIN
        {{ ref('stg_crm_accounts') }} a ON c.CUSTOMER_ID = a.CUSTOMER_ID
    LEFT JOIN
        {{ ref('stg_crm_contacts') }} ct ON c.CUSTOMER_ID = ct.CUSTOMER_ID
    WHERE
        c.CUSTOMER_SEGMENT IN ('Residential', 'SMB', 'Enterprise', 'Government')
        AND c.CUSTOMER_TIER IN ('Bronze', 'Silver', 'Gold', 'Platinum')
        AND c.CUSTOMER_ID IS NOT NULL
        AND (c.EMAIL IS NULL OR c.EMAIL LIKE '%@%.%')
        AND (c.PHONE IS NULL OR c.PHONE REGEXP '^[0-9]{3}-[0-9]{3}-[0-9]{4}$')
        AND (c.ZIP_CODE IS NULL OR c.ZIP_CODE REGEXP '^[0-9]{5}(?:-[0-9]{4})?$')
        AND (c.COUNTRY IS NULL OR c.COUNTRY REGEXP '^[A-Z]{2}$')
        AND c.START_DATE <= CURRENT_DATE
        AND c.STATUS IN ('Active', 'Inactive', 'Suspended', 'Closed')
        AND (a.ACCOUNT_TYPE IS NULL OR a.ACCOUNT_TYPE IN ('Individual', 'Business', 'Government'))
        AND (a.CREATION_DATE IS NULL OR a.CREATION_DATE <= CURRENT_DATE)
        AND (ct.IS_PRIMARY IS NULL OR ct.IS_PRIMARY IN (True, False))
        AND (ct.CONTACT_LABEL IS NULL OR ct.CONTACT_LABEL IN ('Home', 'Work', 'Mobile', 'Other'))
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID']) }} as CustomerSK,
        CUSTOMER_ID,
        TRIM(FIRST_NAME) AS FIRST_NAME,
        TRIM(MIDDLE_NAME) AS MIDDLE_NAME,
        TRIM(LAST_NAME) AS LAST_NAME,
        EMAIL,
        PHONE,
        DATE_OF_BIRTH,
        ADDRESS_LINE1,
        ADDRESS_LINE2,
        CITY,
        STATE,
        ZIP_CODE,
        COUNTRY,
        CUSTOMER_SEGMENT,
        CUSTOMER_TIER,
        START_DATE,
        STATUS,
        ACCOUNT_TYPE,
        CREATION_DATE,
        ACCOUNT_BALANCE,
        BILLING_ADDRESS,
        SHIPPING_ADDRESS,
        CONTACT_TYPE,
        CONTACT_VALUE,
        IS_PRIMARY,
        CONTACT_LABEL,
        START_DATE as EffectiveDate,
        NULL as EndDate
    FROM
        crm_customers_accounts_contacts
)

SELECT * FROM final
