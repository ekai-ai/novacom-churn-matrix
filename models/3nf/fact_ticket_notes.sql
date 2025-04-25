{{ config(materialized='table') }}

with stg_tickets as (
  select
    ticket_id,
    created_at as ticket_created_at -- Assuming staging model standardizes timestamp names
  from {{ ref('stg_sup_tickets') }}
),
stg_notes as (
  select
    note_id,
    ticket_id,
    note_date,
    note_author,
    note_text,
    internal_flag,
    created_at,
    updated_at
  from {{ ref('stg_sup_ticket_notes') }}
),
ranked_notes as (
  select
    n.*,
    t.ticket_created_at,
    row_number() over(partition by n.ticket_id order by n.note_date asc) as note_seq
  from stg_notes n
  join stg_tickets t using(ticket_id)
)
select
  note_id          as note_id,
  ticket_id        as ticket_id,
  note_date        as note_date,
  note_author      as note_author,
  internal_flag    as internal_flag,
  note_text        as note_text,
  created_at       as created_at,
  updated_at       as updated_at,
  case when note_seq = 1
       then datediff('hour', ticket_created_at, note_date)
       end           as first_response_hours,
  datediff(
    'hour',
    lag(note_date) over(partition by ticket_id, note_author order by note_date),
    note_date
  )                 as inter_note_interval_hours
from ranked_notes