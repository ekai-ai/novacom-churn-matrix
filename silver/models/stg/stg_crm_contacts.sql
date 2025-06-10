{{ config(materialized='view') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw', 'CRM_CONTACTS') }}
),

renamed AS (
    SELECT
        CONTACT_ID,
        CUSTOMER_ID,
        CONTACT_TYPE,
        CONTACT_VALUE,
        IS_PRIMARY,
        CONTACT_LABEL,
        NOTE,
        CREATED_AT,
        UPDATED_AT
    FROM
        source
)

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['CONTACT_ID', 'CUSTOMER_ID']) }} AS CONTACT_PK
FROM
    renamed