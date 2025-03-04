-- Customer Acquisition Cost (CAC) Model

with campaign_data as (
    select
        c.campaign_id,
        c.budget,
        t.customer_id,
        t.response_flag
    from {{ source('Combined Source', 'mkt_campaigns') }} as c
    left join {{ source('Combined Source', 'mkt_campaign_targets') }} as t
        on c.campaign_id = t.campaign_id
),
aggregated as (
    select
        sum(budget) as total_budget,
        count(distinct case when response_flag = true then customer_id end) as acquired_customers
    from campaign_data
)

select
    total_budget,
    acquired_customers,
    case
        when acquired_customers = 0 then null
        else total_budget / acquired_customers
    end as customer_acquisition_cost
from aggregated