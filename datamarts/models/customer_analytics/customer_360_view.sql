{{ config(
    materialized='table',
    tags=['customer_analytics', 'customer_360']
) }}

WITH customer_base AS (
    SELECT 
        c.customer_key,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.customer_segment,
        c.customer_tier,
        c.status,
        c.start_date,
        c.city,
        c.state,
        c.country
    FROM {{ source('dimensional_warehouse', 'dimcustomer') }} c
    WHERE c.is_current = 1
),

customer_revenue AS (
    SELECT 
        fi.customer_key,
        COUNT(DISTINCT fi.invoice_id) AS total_invoices,
        SUM(fi.total_amount) AS total_revenue,
        AVG(fi.total_amount) AS avg_invoice_amount,
        MIN(d.full_date) AS first_invoice_date,
        MAX(d.full_date) AS last_invoice_date,
        SUM(CASE WHEN d.full_date >= DATEADD(month, -12, CURRENT_DATE()) THEN fi.total_amount ELSE 0 END) AS revenue_last_12_months,
        SUM(CASE WHEN d.full_date >= DATEADD(month, -3, CURRENT_DATE()) THEN fi.total_amount ELSE 0 END) AS revenue_last_3_months
    FROM {{ source('dimensional_warehouse', 'fact_invoice') }} fi
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} d ON fi.date_key = d.date_key
    GROUP BY fi.customer_key
),

customer_services AS (
    SELECT 
        sa.customer_key,
        COUNT(DISTINCT sa.service_key) AS total_services,
        COUNT(DISTINCT CASE WHEN sa.status = 'Active' THEN sa.service_key END) AS active_services,
        SUM(s.monthly_cost) AS total_monthly_cost,
        LISTAGG(DISTINCT s.service_type, ', ') AS service_types,
        MIN(d.full_date) AS first_service_date,
        MAX(CASE WHEN sa.end_date_key IS NULL THEN d.full_date END) AS latest_service_start
    FROM {{ source('dimensional_warehouse', 'factserviceassignment') }} sa
    JOIN {{ source('dimensional_warehouse', 'dimservice') }} s ON sa.service_key = s.service_key
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} d ON sa.start_date_key = d.date_key
    GROUP BY sa.customer_key
),

customer_support AS (
    SELECT 
        st.customer_key,
        COUNT(DISTINCT st.ticket_id) AS total_tickets,
        COUNT(DISTINCT CASE WHEN sdt.priority = 'High' THEN st.ticket_id END) AS high_priority_tickets,
        COUNT(DISTINCT CASE WHEN sdt.status = 'Resolved' THEN st.ticket_id END) AS resolved_tickets,
        COUNT(DISTINCT CASE WHEN dc.full_date >= DATEADD(month, -6, CURRENT_DATE()) THEN st.ticket_id END) AS tickets_last_6_months,
        AVG(CASE WHEN st.resolution_date_key IS NOT NULL 
             THEN DATEDIFF(day, dc.full_date, dr.full_date) END) AS avg_resolution_days
    FROM {{ source('dimensional_warehouse', 'factsupportticket') }} st
    JOIN {{ source('dimensional_warehouse', 'dimsupportticket') }} sdt ON st.ticket_key = sdt.ticket_key
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} dc ON st.created_date_key = dc.date_key
    LEFT JOIN {{ source('dimensional_warehouse', 'dim_date') }} dr ON st.resolution_date_key = dr.date_key
    GROUP BY st.customer_key
),

customer_campaigns AS (
    SELECT 
        ct.customer_key,
        COUNT(DISTINCT ct.campaign_id) AS total_campaigns_targeted,
        COUNT(DISTINCT CASE WHEN ct.response_flag = 1 THEN ct.campaign_id END) AS campaigns_responded,
        CASE WHEN COUNT(DISTINCT ct.campaign_id) > 0 
             THEN COUNT(DISTINCT CASE WHEN ct.response_flag = 1 THEN ct.campaign_id END) * 100.0 / COUNT(DISTINCT ct.campaign_id)
             ELSE 0 END AS campaign_response_rate,
        MAX(d.full_date) AS last_campaign_date
    FROM {{ source('dimensional_warehouse', 'fact_campaign_target') }} ct
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} d ON ct.assigned_date_key = d.date_key
    GROUP BY ct.customer_key
),

customer_usage AS (
    SELECT 
        sa.customer_key,
        SUM(nu.data_consumed) AS total_data_consumed,
        AVG(nu.data_consumed) AS avg_monthly_data_consumed,
        SUM(nu.usage_cost) AS total_usage_cost,
        COUNT(DISTINCT nu.usage_date_key) AS usage_days_recorded
    FROM {{ source('dimensional_warehouse', 'factnetworkusage') }} nu
    JOIN {{ source('dimensional_warehouse', 'factserviceassignment') }} sa ON nu.assignment_id = sa.assignment_id
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} d ON nu.usage_date_key = d.date_key
    WHERE d.full_date >= DATEADD(month, -12, CURRENT_DATE())
    GROUP BY sa.customer_key
),

customer_churn_indicators AS (
    SELECT 
        cb.customer_key,
        CASE 
            WHEN cb.status = 'Churned' THEN 1
            WHEN cr.last_invoice_date < DATEADD(month, -3, CURRENT_DATE()) THEN 1
            WHEN cs.active_services = 0 THEN 1
            ELSE 0
        END AS is_churned,
        CASE 
            WHEN cb.status = 'Active' 
                 AND cr.revenue_last_3_months = 0 
                 AND cs.total_services > 0 THEN 1
            WHEN cb.status = 'Active' 
                 AND csp.tickets_last_6_months >= 3 THEN 1
            WHEN cb.status = 'Active' 
                 AND cr.revenue_last_12_months < cr.total_revenue * 0.1 THEN 1
            ELSE 0
        END AS churn_risk_flag,
        DATEDIFF(day, cb.start_date, CURRENT_DATE()) AS customer_tenure_days
    FROM customer_base cb
    LEFT JOIN customer_revenue cr ON cb.customer_key = cr.customer_key
    LEFT JOIN customer_services cs ON cb.customer_key = cs.customer_key
    LEFT JOIN customer_support csp ON cb.customer_key = csp.customer_key
)

SELECT 
    cb.customer_key,
    cb.customer_id,
    cb.first_name,
    cb.last_name,
    cb.email,
    cb.phone,
    cb.customer_segment,
    cb.customer_tier,
    cb.status,
    cb.start_date,
    cb.city,
    cb.state,
    cb.country,
    
    -- Revenue metrics
    COALESCE(cr.total_revenue, 0) AS total_revenue,
    COALESCE(cr.revenue_last_12_months, 0) AS revenue_last_12_months,
    COALESCE(cr.revenue_last_3_months, 0) AS revenue_last_3_months,
    COALESCE(cr.avg_invoice_amount, 0) AS avg_invoice_amount,
    COALESCE(cr.total_invoices, 0) AS total_invoices,
    cr.first_invoice_date,
    cr.last_invoice_date,
    
    -- Service metrics
    COALESCE(cs.total_services, 0) AS total_services,
    COALESCE(cs.active_services, 0) AS active_services,
    COALESCE(cs.total_monthly_cost, 0) AS total_monthly_cost,
    cs.service_types,
    cs.first_service_date,
    cs.latest_service_start,
    
    -- Support metrics
    COALESCE(csp.total_tickets, 0) AS total_tickets,
    COALESCE(csp.high_priority_tickets, 0) AS high_priority_tickets,
    COALESCE(csp.resolved_tickets, 0) AS resolved_tickets,
    COALESCE(csp.tickets_last_6_months, 0) AS tickets_last_6_months,
    COALESCE(csp.avg_resolution_days, 0) AS avg_resolution_days,
    
    -- Campaign metrics
    COALESCE(cc.total_campaigns_targeted, 0) AS total_campaigns_targeted,
    COALESCE(cc.campaigns_responded, 0) AS campaigns_responded,
    COALESCE(cc.campaign_response_rate, 0) AS campaign_response_rate,
    cc.last_campaign_date,
    
    -- Usage metrics
    COALESCE(cu.total_data_consumed, 0) AS total_data_consumed_12m,
    COALESCE(cu.avg_monthly_data_consumed, 0) AS avg_monthly_data_consumed,
    COALESCE(cu.total_usage_cost, 0) AS total_usage_cost_12m,
    COALESCE(cu.usage_days_recorded, 0) AS usage_days_recorded,
    
    -- Churn indicators
    cci.is_churned,
    cci.churn_risk_flag,
    cci.customer_tenure_days,
    
    -- Calculated metrics
    CASE 
        WHEN cci.customer_tenure_days > 0 THEN cr.total_revenue / (cci.customer_tenure_days / 365.25)
        ELSE 0 
    END AS annual_revenue_per_customer,
    
    CASE 
        WHEN cs.total_services > 0 THEN cs.active_services * 100.0 / cs.total_services
        ELSE 0 
    END AS service_utilization_rate,
    
    CASE 
        WHEN csp.total_tickets > 0 THEN csp.resolved_tickets * 100.0 / csp.total_tickets
        ELSE 100 
    END AS support_resolution_rate,
    
    CURRENT_TIMESTAMP() AS last_updated

FROM customer_base cb
LEFT JOIN customer_revenue cr ON cb.customer_key = cr.customer_key
LEFT JOIN customer_services cs ON cb.customer_key = cs.customer_key
LEFT JOIN customer_support csp ON cb.customer_key = csp.customer_key
LEFT JOIN customer_campaigns cc ON cb.customer_key = cc.customer_key
LEFT JOIN customer_usage cu ON cb.customer_key = cu.customer_key
LEFT JOIN customer_churn_indicators cci ON cb.customer_key = cci.customer_key