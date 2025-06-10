{{ config(materialized='table', alias='FactSupportTicket', schema='analytics') }}

WITH SupportTickets AS (
    SELECT
        TICKET_ID,
        CUSTOMER_ID,
        CREATED_DATE,
        RESOLUTION_DATE,
        TIMESTAMP_DIFF(RESOLUTION_DATE, CREATED_DATE, SECOND) as TimeToResolutionSeconds
    FROM
        {{ ref('stg_sup_tickets') }}
    WHERE
        TICKET_ID IS NOT NULL AND CREATED_DATE IS NOT NULL
)

SELECT
    TICKET_ID,
    CUSTOMER_ID,
    CAST(FORMAT_DATE('%Y%m%d', CREATED_DATE) AS INT64) AS CREATED_DATE_KEY,
    CAST(FORMAT_DATE('%Y%m%d', RESOLUTION_DATE) AS INT64) AS RESOLUTION_DATE_KEY,
    TimeToResolutionSeconds
FROM
    SupportTickets