/*
  Example: Enhanced Silver Dimension Customer Model
  
  This example shows how to use the silver layer macros to enhance
  the existing silver_dim_customer model with better data quality,
  standardization, and business logic.
*/

-- Use the silver dimension configuration macro
{{ silver_dim_config(
    unique_key='customer_sk',
    sort_keys=['customer_id', 'effective_date'],
    dist_key='customer_id'
) }}

WITH customer_base AS (
    SELECT
        c.CUSTOMER_ID,
        
        -- Use text cleaning macros
        {{ silver_clean_text('c.FIRST_NAME', 50) }} as FIRST_NAME,
        {{ silver_clean_text('c.MIDDLE_NAME', 50) }} as MIDDLE_NAME, 
        {{ silver_clean_text('c.LAST_NAME', 50) }} as LAST_NAME,
        
        -- Use email standardization
        {{ silver_standardize_email('c.EMAIL') }} as EMAIL,
        
        -- Use phone standardization  
        {{ silver_standardize_phone('c.PHONE') }} as PHONE,
        
        c.DATE_OF_BIRTH,
        
        -- Use coalesce chain for address
        {{ silver_coalesce_chain(['c.ADDRESS_LINE1', 'c.ADDRESS_LINE2'], 'No Address') }} as ADDRESS,
        
        {{ silver_clean_text('c.CITY', 100) }} as CITY,
        {{ silver_clean_text('c.STATE', 50) }} as STATE,
        {{ silver_clean_text('c.ZIP_CODE', 20) }} as ZIP_CODE,
        {{ silver_clean_text('c.COUNTRY', 50) }} as COUNTRY,
        
        -- Use status standardization
        {{ silver_status_standardization('c.STATUS') }} as STATUS,
        
        -- Use date standardization
        {{ silver_date_standardization('c.START_DATE') }} as START_DATE,
        
        a.ACCOUNT_TYPE,
        a.ACCOUNT_BALANCE,
        a.CREATION_DATE

    FROM {{ ref('stg_crm_customers') }} c
    LEFT JOIN {{ ref('stg_crm_accounts') }} a ON c.CUSTOMER_ID = a.CUSTOMER_ID
),

enriched_customer AS (
    SELECT 
        *,
        
        -- Generate surrogate key using macro
        {{ silver_generate_surrogate_key(['CUSTOMER_ID']) }} as CUSTOMER_SK,
        
        -- Calculate age using utility macro
        {{ silver_age_calculation('DATE_OF_BIRTH') }} as CUSTOMER_AGE,
        
        -- Calculate tenure using utility macro  
        {{ silver_tenure_calculation('START_DATE', 'CURRENT_DATE()', 'months') }} as TENURE_MONTHS,
        
        -- Apply telecom-specific customer segmentation
        {{ silver_customer_segment_logic('ACCOUNT_BALANCE', '1', 'ACCOUNT_TYPE') }} as CUSTOMER_SEGMENT,
        
        -- Apply customer tier logic
        {{ silver_customer_tier_logic('TENURE_MONTHS', 'ACCOUNT_BALANCE', '85') }} as CUSTOMER_TIER,
        
        -- Add SCD Type 2 columns
        {{ silver_scd_type_2_columns() }}
        
    FROM customer_base
)

SELECT 
    CUSTOMER_SK,
    CUSTOMER_ID,
    FIRST_NAME,
    MIDDLE_NAME,
    LAST_NAME,
    EMAIL,
    PHONE,
    DATE_OF_BIRTH,
    CUSTOMER_AGE,
    ADDRESS,
    CITY,
    STATE,
    ZIP_CODE,
    COUNTRY,
    STATUS,
    START_DATE,
    TENURE_MONTHS,
    ACCOUNT_TYPE,
    ACCOUNT_BALANCE,
    CUSTOMER_SEGMENT,
    CUSTOMER_TIER,
    EFFECTIVE_DATE,
    EXPIRATION_DATE,
    IS_CURRENT,
    ROW_HASH,
    CREATED_AT,
    UPDATED_AT
FROM enriched_customer

/*
  Example: Data Quality Tests Using Macros
  
  Create these as separate test models to validate your silver layer data.
*/

-- Test: silver_dim_customer_quality_checks.sql
-- {{ config(materialized='view', tags=['data_quality', 'test']) }}

-- Check for required fields
-- {{ silver_data_quality_check('silver_dim_customer', 'customer_id', 'not_null') }}
-- UNION ALL
-- {{ silver_data_quality_check('silver_dim_customer', 'customer_id', 'unique') }}
-- UNION ALL 
-- {{ silver_data_quality_check('silver_dim_customer', 'email', 'email') }}

/*
  Example: Fact Table with Macros
  
  Shows how to use macros in fact table development.
*/

-- silver_fact_billing_enhanced.sql
-- {{ silver_fact_config(
--     partition_by=['invoice_date_key'],
--     cluster_by=['customer_id', 'invoice_date_key'],
--     dist_key='customer_id'
-- ) }}

-- WITH billing_base AS (
--     SELECT
--         INVOICE_ID,
--         {{ generate_dim_customer_key('CUSTOMER_ID') }} as CUSTOMER_SK,
--         {{ generate_date_key('INVOICE_DATE') }} as INVOICE_DATE_KEY,
--         
--         -- Standardize amounts
--         {{ silver_amount_standardization('TOTAL_AMOUNT') }} as TOTAL_AMOUNT,
--         {{ silver_amount_standardization('TAX_AMOUNT') }} as TAX_AMOUNT,
--         
--         -- Safe division for calculations
--         {{ silver_safe_divide('PAYMENT_AMOUNT', 'TOTAL_AMOUNT', 0) }} as PAYMENT_RATIO,
--         
--         -- Status standardization
--         {{ silver_status_standardization('STATUS', {'PAID': 'PAID', 'OPEN': 'PENDING'}) }} as INVOICE_STATUS
--         
--     FROM {{ ref('stg_bil_invoices') }}
-- )

-- SELECT * FROM billing_base