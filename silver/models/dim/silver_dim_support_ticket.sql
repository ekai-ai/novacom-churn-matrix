{{ config(materialized='table') }}

with ticket as (
    select
        T.TICKET_ID,
        T.CUSTOMER_ID,
        T.CREATED_DATE,
        T.STATUS,
        T.PRIORITY,
        T.SUBJECT,
        T.DESCRIPTION,
        T.CHANNEL_OF_CONTACT,
        T.ASSIGNED_AGENT,
        T.RESOLUTION_DATE,
        N.NOTE_DATE,
        N.NOTE_AUTHOR,
        N.NOTE_TEXT,
        N.INTERNAL_FLAG
    from {{ ref('stg_sup_tickets') }} as T
    left join {{ ref('stg_sup_ticket_notes') }} as N
        on T.TICKET_ID = N.TICKET_ID
    where T.TICKET_ID is not null
      and T.STATUS in ('Open', 'In Progress', 'Pending Customer', 'Resolved', 'Closed')
      and T.PRIORITY in ('Low', 'Medium', 'High', 'Critical')
      and T.CREATED_DATE is not null
      and T.RESOLUTION_DATE is null
      and T.CHANNEL_OF_CONTACT in ('Phone', 'Email', 'Chat', 'Web Form')
      and (N.INTERNAL_FLAG is null or N.INTERNAL_FLAG in (True, False))
)

select
    -- Generate surrogate key based on TICKET_ID
    TICKET_ID as TicketSK,
    TICKET_ID,
    CUSTOMER_ID,
    CREATED_DATE,
    TRIM(STATUS) as STATUS,
    PRIORITY,
    SUBJECT,
    DESCRIPTION,
    CHANNEL_OF_CONTACT,
    ASSIGNED_AGENT,
    RESOLUTION_DATE,
    NOTE_DATE,
    NOTE_AUTHOR,
    NOTE_TEXT,
    INTERNAL_FLAG
from ticket
