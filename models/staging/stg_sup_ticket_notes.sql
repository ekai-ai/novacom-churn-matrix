{{ config(materialized='table') }}

-- Business Purpose: Staging model for SUP_TICKET_NOTES raw data.
-- Enforces non-null NOTE_ID, NOTE_DATE, NOTE_AUTHOR and standardizes column names for downstream 3NF models.

select
  NOTE_ID        as note_id,
  TICKET_ID      as ticket_id,
  NOTE_DATE      as note_date,
  NOTE_AUTHOR    as note_author,
  NOTE_TEXT      as note_text,
  INTERNAL_FLAG  as internal_flag,
  CREATED_AT     as created_at,
  UPDATED_AT     as updated_at
from {{ source('raw', 'SUP_TICKET_NOTES') }}
where NOTE_ID    is not null
  and NOTE_DATE  is not null
  and NOTE_AUTHOR is not null;