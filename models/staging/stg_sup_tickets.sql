{{ config(
    materialized='table'
) }}

-- Staging clean and standardize SUP_TICKETS source
with raw_tickets as (
    select
        ticket_id           ::int64    as ticket_id,
        customer_id         ::int64    as customer_id,
        created_date        ::timestamp as created_date,
        status                        as status,
        priority                      as priority,
        subject                       as subject,
        description                   as description,
        channel_of_contact            as channel_of_contact,
        assigned_agent                as assigned_agent,
        resolution_date     ::timestamp as resolution_date,
        created_at          ::timestamp as created_at,
        updated_at          ::timestamp as updated_at
    from {{ source('raw', 'sup_tickets') }}
)

select distinct *
from raw_tickets;