{{ config(
    materialized='table',
    tags=['customer_analytics', 'churn_prediction', 'ml_features']
) }}

WITH customer_base AS (
    SELECT 
        customer_key,
        customer_id,
        customer_segment,
        customer_tier,
        status,
        customer_tenure_days,
        is_churned,
        churn_risk_flag
    FROM {{ ref('customer_360_view') }}
),

revenue_features AS (
    SELECT 
        fi.customer_key,
        
        -- Revenue trends (last 12 months)
        SUM(CASE WHEN d.full_date >= DATEADD(month, -1, CURRENT_DATE()) THEN fi.total_amount ELSE 0 END) AS revenue_last_1_month,
        SUM(CASE WHEN d.full_date >= DATEADD(month, -3, CURRENT_DATE()) THEN fi.total_amount ELSE 0 END) AS revenue_last_3_months,
        SUM(CASE WHEN d.full_date >= DATEADD(month, -6, CURRENT_DATE()) THEN fi.total_amount ELSE 0 END) AS revenue_last_6_months,
        SUM(CASE WHEN d.full_date >= DATEADD(month, -12, CURRENT_DATE()) THEN fi.total_amount ELSE 0 END) AS revenue_last_12_months,
        
        -- Revenue patterns
        COUNT(DISTINCT CASE WHEN d.full_date >= DATEADD(month, -12, CURRENT_DATE()) THEN d.month END) AS months_with_revenue_12m,
        AVG(CASE WHEN d.full_date >= DATEADD(month, -12, CURRENT_DATE()) THEN fi.total_amount END) AS avg_monthly_revenue,
        STDDEV(CASE WHEN d.full_date >= DATEADD(month, -12, CURRENT_DATE()) THEN fi.total_amount END) AS revenue_volatility,
        
        -- Payment behavior
        COUNT(CASE WHEN d.full_date >= DATEADD(month, -6, CURRENT_DATE()) THEN fi.invoice_id END) AS invoice_count_6m,
        COUNT(CASE WHEN d.full_date >= DATEADD(month, -6, CURRENT_DATE()) AND fi.status = 'Overdue' THEN fi.invoice_id END) AS overdue_invoices_6m,
        MAX(d.full_date) AS last_payment_date,
        DATEDIFF(day, MAX(d.full_date), CURRENT_DATE()) AS days_since_last_payment
        
    FROM {{ source('dimensional_warehouse', 'fact_invoice') }} fi
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} d ON fi.date_key = d.date_key
    WHERE d.full_date >= DATEADD(month, -12, CURRENT_DATE())
    GROUP BY fi.customer_key
),

service_features AS (
    SELECT 
        sa.customer_key,
        
        -- Service portfolio
        COUNT(DISTINCT sa.service_key) AS total_services_ever,
        COUNT(DISTINCT CASE WHEN sa.status = 'Active' THEN sa.service_key END) AS active_services_count,
        COUNT(DISTINCT CASE WHEN sa.status = 'Terminated' THEN sa.service_key END) AS terminated_services_count,
        COUNT(DISTINCT s.service_type) AS unique_service_types,
        
        -- Service changes (last 12 months)
        COUNT(CASE WHEN d_start.full_date >= DATEADD(month, -12, CURRENT_DATE()) THEN sa.assignment_id END) AS new_services_12m,
        COUNT(CASE WHEN d_end.full_date >= DATEADD(month, -12, CURRENT_DATE()) THEN sa.assignment_id END) AS terminated_services_12m,
        
        -- Service value
        SUM(CASE WHEN sa.status = 'Active' THEN s.monthly_cost ELSE 0 END) AS current_monthly_cost,
        AVG(s.monthly_cost) AS avg_service_cost,
        MAX(s.monthly_cost) AS max_service_cost,
        
        -- Service tenure
        AVG(DATEDIFF(day, d_start.full_date, COALESCE(d_end.full_date, CURRENT_DATE()))) AS avg_service_tenure_days,
        MIN(d_start.full_date) AS first_service_date,
        MAX(d_start.full_date) AS latest_service_start_date,
        
        -- Service types (binary flags)
        MAX(CASE WHEN s.service_type = 'Internet' THEN 1 ELSE 0 END) AS has_internet,
        MAX(CASE WHEN s.service_type = 'TV' THEN 1 ELSE 0 END) AS has_tv,
        MAX(CASE WHEN s.service_type = 'Mobile' THEN 1 ELSE 0 END) AS has_mobile,
        MAX(CASE WHEN s.service_type = 'Voice' THEN 1 ELSE 0 END) AS has_voice
        
    FROM {{ source('dimensional_warehouse', 'factserviceassignment') }} sa
    JOIN {{ source('dimensional_warehouse', 'dimservice') }} s ON sa.service_key = s.service_key
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} d_start ON sa.start_date_key = d_start.date_key
    LEFT JOIN {{ source('dimensional_warehouse', 'dim_date') }} d_end ON sa.end_date_key = d_end.date_key
    GROUP BY sa.customer_key
),

support_features AS (
    SELECT 
        st.customer_key,
        
        -- Ticket volume and trends
        COUNT(st.ticket_id) AS total_tickets_all_time,
        COUNT(CASE WHEN dc.full_date >= DATEADD(month, -3, CURRENT_DATE()) THEN st.ticket_id END) AS tickets_last_3_months,
        COUNT(CASE WHEN dc.full_date >= DATEADD(month, -6, CURRENT_DATE()) THEN st.ticket_id END) AS tickets_last_6_months,
        COUNT(CASE WHEN dc.full_date >= DATEADD(month, -12, CURRENT_DATE()) THEN st.ticket_id END) AS tickets_last_12_months,
        
        -- Ticket severity and resolution
        COUNT(CASE WHEN sdt.priority = 'High' THEN st.ticket_id END) AS high_priority_tickets,
        COUNT(CASE WHEN sdt.priority = 'Medium' THEN st.ticket_id END) AS medium_priority_tickets,
        COUNT(CASE WHEN sdt.status = 'Resolved' THEN st.ticket_id END) AS resolved_tickets,
        COUNT(CASE WHEN sdt.status = 'Open' THEN st.ticket_id END) AS open_tickets,
        
        -- Resolution patterns
        AVG(CASE WHEN st.resolution_date_key IS NOT NULL 
             THEN DATEDIFF(day, dc.full_date, dr.full_date) END) AS avg_resolution_time_days,
        
        -- Recent support activity
        MAX(dc.full_date) AS last_ticket_date,
        DATEDIFF(day, MAX(dc.full_date), CURRENT_DATE()) AS days_since_last_ticket,
        
        -- Escalation patterns
        COUNT(CASE WHEN dc.full_date >= DATEADD(month, -6, CURRENT_DATE()) 
                   AND sdt.priority = 'High' THEN st.ticket_id END) AS escalated_tickets_6m
        
    FROM {{ source('dimensional_warehouse', 'factsupportticket') }} st
    JOIN {{ source('dimensional_warehouse', 'dimsupportticket') }} sdt ON st.ticket_key = sdt.ticket_key
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} dc ON st.created_date_key = dc.date_key
    LEFT JOIN {{ source('dimensional_warehouse', 'dim_date') }} dr ON st.resolution_date_key = dr.date_key
    GROUP BY st.customer_key
),

usage_features AS (
    SELECT 
        sa.customer_key,
        
        -- Usage volume trends
        SUM(CASE WHEN d.full_date >= DATEADD(month, -1, CURRENT_DATE()) THEN nu.data_consumed ELSE 0 END) AS data_usage_last_1_month,
        SUM(CASE WHEN d.full_date >= DATEADD(month, -3, CURRENT_DATE()) THEN nu.data_consumed ELSE 0 END) AS data_usage_last_3_months,
        SUM(CASE WHEN d.full_date >= DATEADD(month, -6, CURRENT_DATE()) THEN nu.data_consumed ELSE 0 END) AS data_usage_last_6_months,
        
        -- Usage patterns
        COUNT(DISTINCT CASE WHEN d.full_date >= DATEADD(month, -3, CURRENT_DATE()) THEN d.date_key END) AS usage_days_last_3_months,
        AVG(CASE WHEN d.full_date >= DATEADD(month, -3, CURRENT_DATE()) THEN nu.data_consumed END) AS avg_daily_usage_3m,
        STDDEV(CASE WHEN d.full_date >= DATEADD(month, -3, CURRENT_DATE()) THEN nu.data_consumed END) AS usage_volatility_3m,
        
        -- Usage costs
        SUM(CASE WHEN d.full_date >= DATEADD(month, -3, CURRENT_DATE()) THEN nu.usage_cost ELSE 0 END) AS usage_cost_last_3_months,
        
        -- Usage trends (declining usage indicator)
        CASE WHEN SUM(CASE WHEN d.full_date >= DATEADD(month, -1, CURRENT_DATE()) THEN nu.data_consumed ELSE 0 END) = 0 THEN 1 ELSE 0 END AS zero_usage_last_month,
        
        -- Recent usage activity
        MAX(d.full_date) AS last_usage_date,
        DATEDIFF(day, MAX(d.full_date), CURRENT_DATE()) AS days_since_last_usage
        
    FROM {{ source('dimensional_warehouse', 'factnetworkusage') }} nu
    JOIN {{ source('dimensional_warehouse', 'factserviceassignment') }} sa ON nu.assignment_id = sa.assignment_id
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} d ON nu.usage_date_key = d.date_key
    WHERE d.full_date >= DATEADD(month, -6, CURRENT_DATE())
    GROUP BY sa.customer_key
),

campaign_features AS (
    SELECT 
        ct.customer_key,
        
        -- Campaign engagement
        COUNT(DISTINCT ct.campaign_id) AS total_campaigns_targeted,
        COUNT(CASE WHEN ct.response_flag = 1 THEN ct.campaign_id END) AS campaigns_responded,
        COUNT(CASE WHEN d.full_date >= DATEADD(month, -6, CURRENT_DATE()) THEN ct.campaign_id END) AS campaigns_last_6_months,
        COUNT(CASE WHEN d.full_date >= DATEADD(month, -6, CURRENT_DATE()) AND ct.response_flag = 1 THEN ct.campaign_id END) AS responses_last_6_months,
        
        -- Response patterns
        CASE WHEN COUNT(DISTINCT ct.campaign_id) > 0 
             THEN COUNT(CASE WHEN ct.response_flag = 1 THEN ct.campaign_id END) * 100.0 / COUNT(DISTINCT ct.campaign_id)
             ELSE 0 END AS overall_response_rate,
        
        -- Recent campaign activity
        MAX(d.full_date) AS last_campaign_date,
        DATEDIFF(day, MAX(d.full_date), CURRENT_DATE()) AS days_since_last_campaign
        
    FROM {{ source('dimensional_warehouse', 'fact_campaign_target') }} ct
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} d ON ct.assigned_date_key = d.date_key
    GROUP BY ct.customer_key
),

outage_impact AS (
    SELECT 
        sa.customer_key,
        
        -- Outage exposure
        COUNT(DISTINCT no.outage_id) AS outages_experienced_12m,
        SUM(DATEDIFF(hour, ds.full_date, de.full_date)) AS total_outage_hours_12m,
        AVG(DATEDIFF(hour, ds.full_date, de.full_date)) AS avg_outage_duration_hours,
        COUNT(CASE WHEN no.outage_type = 'Planned' THEN no.outage_id END) AS planned_outages_12m,
        COUNT(CASE WHEN no.outage_type = 'Unplanned' THEN no.outage_id END) AS unplanned_outages_12m,
        
        -- Recent outage impact
        MAX(ds.full_date) AS last_outage_date,
        DATEDIFF(day, MAX(ds.full_date), CURRENT_DATE()) AS days_since_last_outage
        
    FROM {{ source('dimensional_warehouse', 'factnetworkoutage') }} no
    JOIN {{ source('dimensional_warehouse', 'factserviceassignment') }} sa ON no.assignment_id = sa.assignment_id
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} ds ON no.start_date_key = ds.date_key
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} de ON no.end_date_key = de.date_key
    WHERE ds.full_date >= DATEADD(month, -12, CURRENT_DATE())
    GROUP BY sa.customer_key
)

SELECT 
    cb.customer_key,
    cb.customer_id,
    cb.customer_segment,
    cb.customer_tier,
    cb.status,
    cb.customer_tenure_days,
    cb.is_churned,
    cb.churn_risk_flag,
    
    -- Revenue features
    COALESCE(rf.revenue_last_1_month, 0) AS revenue_last_1_month,
    COALESCE(rf.revenue_last_3_months, 0) AS revenue_last_3_months,
    COALESCE(rf.revenue_last_6_months, 0) AS revenue_last_6_months,
    COALESCE(rf.revenue_last_12_months, 0) AS revenue_last_12_months,
    COALESCE(rf.months_with_revenue_12m, 0) AS months_with_revenue_12m,
    COALESCE(rf.avg_monthly_revenue, 0) AS avg_monthly_revenue,
    COALESCE(rf.revenue_volatility, 0) AS revenue_volatility,
    COALESCE(rf.invoice_count_6m, 0) AS invoice_count_6m,
    COALESCE(rf.overdue_invoices_6m, 0) AS overdue_invoices_6m,
    COALESCE(rf.days_since_last_payment, 999) AS days_since_last_payment,
    
    -- Service features
    COALESCE(sf.total_services_ever, 0) AS total_services_ever,
    COALESCE(sf.active_services_count, 0) AS active_services_count,
    COALESCE(sf.terminated_services_count, 0) AS terminated_services_count,
    COALESCE(sf.unique_service_types, 0) AS unique_service_types,
    COALESCE(sf.new_services_12m, 0) AS new_services_12m,
    COALESCE(sf.terminated_services_12m, 0) AS terminated_services_12m,
    COALESCE(sf.current_monthly_cost, 0) AS current_monthly_cost,
    COALESCE(sf.avg_service_cost, 0) AS avg_service_cost,
    COALESCE(sf.max_service_cost, 0) AS max_service_cost,
    COALESCE(sf.avg_service_tenure_days, 0) AS avg_service_tenure_days,
    COALESCE(sf.has_internet, 0) AS has_internet,
    COALESCE(sf.has_tv, 0) AS has_tv,
    COALESCE(sf.has_mobile, 0) AS has_mobile,
    COALESCE(sf.has_voice, 0) AS has_voice,
    
    -- Support features
    COALESCE(spf.total_tickets_all_time, 0) AS total_tickets_all_time,
    COALESCE(spf.tickets_last_3_months, 0) AS tickets_last_3_months,
    COALESCE(spf.tickets_last_6_months, 0) AS tickets_last_6_months,
    COALESCE(spf.tickets_last_12_months, 0) AS tickets_last_12_months,
    COALESCE(spf.high_priority_tickets, 0) AS high_priority_tickets,
    COALESCE(spf.medium_priority_tickets, 0) AS medium_priority_tickets,
    COALESCE(spf.resolved_tickets, 0) AS resolved_tickets,
    COALESCE(spf.open_tickets, 0) AS open_tickets,
    COALESCE(spf.avg_resolution_time_days, 0) AS avg_resolution_time_days,
    COALESCE(spf.days_since_last_ticket, 999) AS days_since_last_ticket,
    COALESCE(spf.escalated_tickets_6m, 0) AS escalated_tickets_6m,
    
    -- Usage features
    COALESCE(uf.data_usage_last_1_month, 0) AS data_usage_last_1_month,
    COALESCE(uf.data_usage_last_3_months, 0) AS data_usage_last_3_months,
    COALESCE(uf.data_usage_last_6_months, 0) AS data_usage_last_6_months,
    COALESCE(uf.usage_days_last_3_months, 0) AS usage_days_last_3_months,
    COALESCE(uf.avg_daily_usage_3m, 0) AS avg_daily_usage_3m,
    COALESCE(uf.usage_volatility_3m, 0) AS usage_volatility_3m,
    COALESCE(uf.usage_cost_last_3_months, 0) AS usage_cost_last_3_months,
    COALESCE(uf.zero_usage_last_month, 0) AS zero_usage_last_month,
    COALESCE(uf.days_since_last_usage, 999) AS days_since_last_usage,
    
    -- Campaign features
    COALESCE(cf.total_campaigns_targeted, 0) AS total_campaigns_targeted,
    COALESCE(cf.campaigns_responded, 0) AS campaigns_responded,
    COALESCE(cf.campaigns_last_6_months, 0) AS campaigns_last_6_months,
    COALESCE(cf.responses_last_6_months, 0) AS responses_last_6_months,
    COALESCE(cf.overall_response_rate, 0) AS overall_response_rate,
    COALESCE(cf.days_since_last_campaign, 999) AS days_since_last_campaign,
    
    -- Outage impact features
    COALESCE(oi.outages_experienced_12m, 0) AS outages_experienced_12m,
    COALESCE(oi.total_outage_hours_12m, 0) AS total_outage_hours_12m,
    COALESCE(oi.avg_outage_duration_hours, 0) AS avg_outage_duration_hours,
    COALESCE(oi.planned_outages_12m, 0) AS planned_outages_12m,
    COALESCE(oi.unplanned_outages_12m, 0) AS unplanned_outages_12m,
    COALESCE(oi.days_since_last_outage, 999) AS days_since_last_outage,
    
    -- Calculated risk indicators
    CASE 
        WHEN COALESCE(rf.revenue_last_3_months, 0) = 0 THEN 1 
        ELSE 0 
    END AS no_revenue_3m_flag,
    
    CASE 
        WHEN COALESCE(sf.terminated_services_12m, 0) > 0 THEN 1 
        ELSE 0 
    END AS service_termination_flag,
    
    CASE 
        WHEN COALESCE(spf.tickets_last_6_months, 0) >= 3 THEN 1 
        ELSE 0 
    END AS high_support_volume_flag,
    
    CASE 
        WHEN COALESCE(uf.zero_usage_last_month, 0) = 1 THEN 1 
        ELSE 0 
    END AS zero_usage_flag,
    
    CASE 
        WHEN COALESCE(rf.overdue_invoices_6m, 0) > 0 THEN 1 
        ELSE 0 
    END AS payment_issues_flag,
    
    CURRENT_TIMESTAMP() AS feature_extraction_date

FROM customer_base cb
LEFT JOIN revenue_features rf ON cb.customer_key = rf.customer_key
LEFT JOIN service_features sf ON cb.customer_key = sf.customer_key
LEFT JOIN support_features spf ON cb.customer_key = spf.customer_key
LEFT JOIN usage_features uf ON cb.customer_key = uf.customer_key
LEFT JOIN campaign_features cf ON cb.customer_key = cf.customer_key
LEFT JOIN outage_impact oi ON cb.customer_key = oi.customer_key