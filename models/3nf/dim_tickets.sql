{{ config(
    materialized='table',
    tags=['3nf','dimensional']
) }}

-- Selecting final columns from the staging model for tickets.
-- Renaming and basic cleaning are assumed to happen in stg_sup_tickets.
select
    ticket_id,
    customer_id,
    created_date,
    status,
    priority,
    channel_of_contact,
    assigned_agent,
    resolution_date
from {{ ref('stg_sup_tickets') }}