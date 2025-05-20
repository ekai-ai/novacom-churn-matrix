{{ config(materialized='table') }}

with source_data as (
    select *
    from {{ source('raw', 'SUP_TICKET_NOTES') }}
)

select
    NOTE_ID,
    TICKET_ID,
    NOTE_DATE,
    md5(NOTE_AUTHOR) as NOTE_AUTHOR,
    NOTE_TEXT,
    INTERNAL_FLAG,
    CREATED_AT,
    UPDATED_AT
from source_data