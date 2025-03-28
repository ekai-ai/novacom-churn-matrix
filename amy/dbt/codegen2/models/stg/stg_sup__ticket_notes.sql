{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT
        NOTE_ID,
        TICKET_ID,
        NOTE_DATE,
        NOTE_AUTHOR,
        NOTE_TEXT,
        INTERNAL_FLAG,
        CREATED_AT,
        UPDATED_AT
    FROM {{ source('raw', 'SUP_TICKET_NOTES') }}
)

SELECT
    NOTE_ID,
    TICKET_ID,
    NOTE_DATE,
    NOTE_AUTHOR,
    NOTE_TEXT,
    INTERNAL_FLAG,
    CREATED_AT,
    UPDATED_AT
FROM source