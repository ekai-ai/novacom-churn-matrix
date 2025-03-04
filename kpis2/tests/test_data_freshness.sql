-- Test to ensure source data is fresh
-- Checks if the most recent data is within an acceptable time range

{% set max_days_stale = 7 %}

with source_freshness as (
    select
        max(invoice_date) as most_recent_date
    from {{ source('Combined Source', 'bil_invoices') }}
),

validation as (
    select
        most_recent_date,
        current_date - most_recent_date as days_stale
    from source_freshness
)

select *
from validation
where days_stale > {{ max_days_stale }}
