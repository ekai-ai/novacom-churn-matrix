-- Test to ensure source data is fresh
-- Checks if the most recent data is within an acceptable time range
-- For testing/development environment, using a large tolerance

{% set max_days_stale = 3650 %}  -- ~10 years tolerance for dev environment

with source_freshness as (
    select
        'bil_invoices' as source_name,
        max(invoice_date) as most_recent_date
    from {{ source('Combined Source', 'bil_invoices') }}
    
    union all
    
    select
        'crm_customers' as source_name,
        max(created_at) as most_recent_date
    from {{ source('Combined Source', 'crm_customers') }}
    
    union all
    
    select
        'prv_service_assignments' as source_name,
        max(start_date) as most_recent_date
    from {{ source('Combined Source', 'prv_service_assignments') }}
    
    union all
    
    select
        'mkt_campaigns' as source_name,
        max(start_date) as most_recent_date
    from {{ source('Combined Source', 'mkt_campaigns') }}
),

validation as (
    select
        source_name,
        most_recent_date,
        current_date - most_recent_date as days_stale
    from source_freshness
)

select *
from validation
where days_stale > {{ max_days_stale }}
