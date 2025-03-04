-- Test to ensure source data is fresh
-- Checks if the most recent data is within an acceptable time range
-- For testing/development environment, using a large tolerance

  -- ~10 years tolerance for dev environment

with source_freshness as (
    select
        max(invoice_date) as most_recent_date
    from NOVACOM.BRONZE.bil_invoices
),

validation as (
    select
        most_recent_date,
        current_date - most_recent_date as days_stale
    from source_freshness
)

select *
from validation
where days_stale > 3650