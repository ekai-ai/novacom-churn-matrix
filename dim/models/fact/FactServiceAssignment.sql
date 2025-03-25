{{ config(materialized='table', alias='FactServiceAssignment', schema='analytics') }}

WITH ServiceAssignments AS (
    SELECT
        ASSIGNMENT_ID,
        SERVICE_ID,
        CUSTOMER_ID,
        START_DATE,
        END_DATE,
        STATUS,
        PROVISIONING_STATUS
    FROM
        {{ ref('stg_prv_service_assignments') }}
    WHERE
        ASSIGNMENT_ID IS NOT NULL
        AND SERVICE_ID IS NOT NULL
        AND CUSTOMER_ID IS NOT NULL
),

DimDateStart AS (
    SELECT
        date_day as start_date,
        date_key as start_date_key
    FROM
       {{ ref('dim_date') }} 
),

DimDateEnd AS (
    SELECT
        date_day as end_date,
        date_key as end_date_key
    FROM
       {{ ref('dim_date') }} 
)


SELECT
    sa.ASSIGNMENT_ID,
    sa.SERVICE_ID,
    sa.CUSTOMER_ID,
    ds.start_date_key AS START_DATE_KEY,
    de.end_date_key AS END_DATE_KEY,
    sa.STATUS,
    sa.PROVISIONING_STATUS

FROM
    ServiceAssignments sa
LEFT JOIN
    DimDateStart ds ON sa.START_DATE = ds.start_date
LEFT JOIN
    DimDateEnd de ON sa.END_DATE = de.end_date
WHERE ds.start_date_key IS NOT NULL
