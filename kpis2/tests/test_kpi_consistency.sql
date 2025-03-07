-- Test to ensure consistency between related KPIs
-- For example, services with high adoption rates should generally have lower acquisition costs

-- Compare service adoption rate with customer acquisition cost
-- This test checks if there are any services with both low adoption rate and high acquisition cost,
-- which might indicate inefficient marketing spend

with service_adoption as (
    select
        service_id,
        service_name,
        adoption_rate
    from {{ ref('service_adoption_rate') }}
),

acquisition_cost as (
    select
        customer_acquisition_cost
    from {{ ref('customer_acquisition_cost') }}
),

-- Define thresholds for what constitutes "low adoption" and "high cost"
thresholds as (
    select
        0.2 as low_adoption_threshold,  -- Services with less than 20% adoption are considered low
        (select avg(customer_acquisition_cost) * 1.5 from acquisition_cost) as high_cost_threshold
)

-- Identify potential inconsistencies
select
    'Low adoption rate with high acquisition cost' as inconsistency_type,
    sa.service_id,
    sa.service_name,
    sa.adoption_rate,
    ac.customer_acquisition_cost
from service_adoption sa
cross join acquisition_cost ac
cross join thresholds t
where sa.adoption_rate < t.low_adoption_threshold
  and ac.customer_acquisition_cost > t.high_cost_threshold
