{{ config(
    materialized='table',
    tags=['network_ops', 'reliability', 'sla_monitoring']
) }}

WITH service_assignments_with_details AS (
    SELECT 
        sa.assignment_id,
        sa.customer_key,
        sa.service_key,
        sa.status,
        c.customer_segment,
        c.customer_tier,
        c.state,
        c.city,
        s.service_type,
        s.service_name,
        s.monthly_cost,
        ds.full_date AS start_date,
        de.full_date AS end_date,
        CASE WHEN sa.end_date_key IS NULL THEN CURRENT_DATE() ELSE de.full_date END AS effective_end_date
    FROM {{ source('dimensional_warehouse', 'factserviceassignment') }} sa
    JOIN {{ source('dimensional_warehouse', 'dimcustomer') }} c ON sa.customer_key = c.customer_key
    JOIN {{ source('dimensional_warehouse', 'dimservice') }} s ON sa.service_key = s.service_key
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} ds ON sa.start_date_key = ds.date_key
    LEFT JOIN {{ source('dimensional_warehouse', 'dim_date') }} de ON sa.end_date_key = de.date_key
    WHERE ds.full_date >= DATEADD(month, -24, CURRENT_DATE())
),

outage_impact_by_assignment AS (
    SELECT 
        no.assignment_id,
        COUNT(DISTINCT no.outage_id) AS total_outages,
        COUNT(DISTINCT CASE WHEN no.outage_type = 'Unplanned' THEN no.outage_id END) AS unplanned_outages,
        COUNT(DISTINCT CASE WHEN no.outage_type = 'Planned' THEN no.outage_id END) AS planned_outages,
        SUM(DATEDIFF(minute, ds.full_date, de.full_date)) AS total_outage_minutes,
        AVG(DATEDIFF(minute, ds.full_date, de.full_date)) AS avg_outage_duration_minutes,
        MAX(DATEDIFF(minute, ds.full_date, de.full_date)) AS max_outage_duration_minutes,
        
        -- Recent outage activity (last 90 days)
        COUNT(CASE WHEN ds.full_date >= DATEADD(day, -90, CURRENT_DATE()) THEN no.outage_id END) AS outages_last_90_days,
        SUM(CASE WHEN ds.full_date >= DATEADD(day, -90, CURRENT_DATE()) 
             THEN DATEDIFF(minute, ds.full_date, de.full_date) ELSE 0 END) AS outage_minutes_last_90_days,
        
        -- Outage frequency patterns
        COUNT(CASE WHEN ds.day_of_week IN (1, 7) THEN no.outage_id END) AS weekend_outages,
        COUNT(CASE WHEN ds.hour BETWEEN 9 AND 17 THEN no.outage_id END) AS business_hours_outages,
        
        -- Most recent outage
        MAX(ds.full_date) AS last_outage_date,
        DATEDIFF(day, MAX(ds.full_date), CURRENT_DATE()) AS days_since_last_outage
        
    FROM {{ source('dimensional_warehouse', 'factnetworkoutage') }} no
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} ds ON no.start_date_key = ds.date_key
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} de ON no.end_date_key = de.date_key
    WHERE ds.full_date >= DATEADD(month, -24, CURRENT_DATE())
    GROUP BY no.assignment_id
),

usage_patterns AS (
    SELECT 
        nu.assignment_id,
        COUNT(DISTINCT d.date_key) AS total_usage_days,
        AVG(nu.data_consumed) AS avg_daily_usage,
        STDDEV(nu.data_consumed) AS usage_volatility,
        MAX(nu.usage_peak) AS peak_usage,
        SUM(nu.usage_cost) AS total_usage_cost,
        
        -- Recent usage trends (last 30 days)
        COUNT(CASE WHEN d.full_date >= DATEADD(day, -30, CURRENT_DATE()) THEN d.date_key END) AS usage_days_last_30,
        AVG(CASE WHEN d.full_date >= DATEADD(day, -30, CURRENT_DATE()) THEN nu.data_consumed END) AS avg_usage_last_30_days,
        
        -- Usage consistency
        COUNT(DISTINCT d.month) AS months_with_usage,
        MAX(d.full_date) AS last_usage_date,
        DATEDIFF(day, MAX(d.full_date), CURRENT_DATE()) AS days_since_last_usage
        
    FROM {{ source('dimensional_warehouse', 'factnetworkusage') }} nu
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} d ON nu.usage_date_key = d.date_key
    WHERE d.full_date >= DATEADD(month, -24, CURRENT_DATE())
    GROUP BY nu.assignment_id
),

support_incidents AS (
    SELECT 
        sa.assignment_id,
        COUNT(st.ticket_id) AS total_support_tickets,
        COUNT(CASE WHEN sdt.priority = 'High' THEN st.ticket_id END) AS high_priority_tickets,
        COUNT(CASE WHEN sdt.priority = 'Critical' THEN st.ticket_id END) AS critical_tickets,
        AVG(CASE WHEN st.resolution_date_key IS NOT NULL 
             THEN DATEDIFF(hour, dc.full_date, dr.full_date) END) AS avg_resolution_hours,
        
        -- Recent support activity (last 90 days)
        COUNT(CASE WHEN dc.full_date >= DATEADD(day, -90, CURRENT_DATE()) THEN st.ticket_id END) AS tickets_last_90_days,
        
        -- Support patterns
        MAX(dc.full_date) AS last_ticket_date,
        DATEDIFF(day, MAX(dc.full_date), CURRENT_DATE()) AS days_since_last_ticket
        
    FROM service_assignments_with_details sa
    JOIN {{ source('dimensional_warehouse', 'factsupportticket') }} st ON sa.customer_key = st.customer_key
    JOIN {{ source('dimensional_warehouse', 'dimsupportticket') }} sdt ON st.ticket_key = sdt.ticket_key
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} dc ON st.created_date_key = dc.date_key
    LEFT JOIN {{ source('dimensional_warehouse', 'dim_date') }} dr ON st.resolution_date_key = dr.date_key
    GROUP BY sa.assignment_id
),

service_reliability_scores AS (
    SELECT 
        sa.assignment_id,
        sa.customer_key,
        sa.service_key,
        sa.customer_segment,
        sa.customer_tier,
        sa.service_type,
        sa.service_name,
        sa.monthly_cost,
        sa.state,
        sa.city,
        sa.start_date,
        sa.end_date,
        sa.status,
        
        -- Service tenure
        DATEDIFF(day, sa.start_date, sa.effective_end_date) AS service_tenure_days,
        
        -- Outage metrics
        COALESCE(oi.total_outages, 0) AS total_outages,
        COALESCE(oi.unplanned_outages, 0) AS unplanned_outages,
        COALESCE(oi.planned_outages, 0) AS planned_outages,
        COALESCE(oi.total_outage_minutes, 0) AS total_outage_minutes,
        COALESCE(oi.avg_outage_duration_minutes, 0) AS avg_outage_duration_minutes,
        COALESCE(oi.max_outage_duration_minutes, 0) AS max_outage_duration_minutes,
        COALESCE(oi.outages_last_90_days, 0) AS outages_last_90_days,
        COALESCE(oi.outage_minutes_last_90_days, 0) AS outage_minutes_last_90_days,
        COALESCE(oi.weekend_outages, 0) AS weekend_outages,
        COALESCE(oi.business_hours_outages, 0) AS business_hours_outages,
        COALESCE(oi.days_since_last_outage, 999) AS days_since_last_outage,
        
        -- Usage metrics
        COALESCE(up.total_usage_days, 0) AS total_usage_days,
        COALESCE(up.avg_daily_usage, 0) AS avg_daily_usage,
        COALESCE(up.usage_volatility, 0) AS usage_volatility,
        COALESCE(up.peak_usage, 0) AS peak_usage,
        COALESCE(up.total_usage_cost, 0) AS total_usage_cost,
        COALESCE(up.usage_days_last_30, 0) AS usage_days_last_30,
        COALESCE(up.avg_usage_last_30_days, 0) AS avg_usage_last_30_days,
        COALESCE(up.months_with_usage, 0) AS months_with_usage,
        COALESCE(up.days_since_last_usage, 999) AS days_since_last_usage,
        
        -- Support metrics
        COALESCE(si.total_support_tickets, 0) AS total_support_tickets,
        COALESCE(si.high_priority_tickets, 0) AS high_priority_tickets,
        COALESCE(si.critical_tickets, 0) AS critical_tickets,
        COALESCE(si.avg_resolution_hours, 0) AS avg_resolution_hours,
        COALESCE(si.tickets_last_90_days, 0) AS tickets_last_90_days,
        COALESCE(si.days_since_last_ticket, 999) AS days_since_last_ticket,
        
        -- Calculated reliability metrics
        
        -- Availability percentage (last 90 days)
        CASE 
            WHEN DATEDIFF(day, sa.start_date, sa.effective_end_date) >= 90 
            THEN 100.0 - (COALESCE(oi.outage_minutes_last_90_days, 0) / (90.0 * 24 * 60) * 100)
            ELSE 100.0 - (COALESCE(oi.total_outage_minutes, 0) / (DATEDIFF(day, sa.start_date, sa.effective_end_date) * 24.0 * 60) * 100)
        END AS availability_percentage,
        
        -- Mean Time Between Failures (MTBF) in days
        CASE 
            WHEN COALESCE(oi.unplanned_outages, 0) > 0 
            THEN DATEDIFF(day, sa.start_date, sa.effective_end_date) / COALESCE(oi.unplanned_outages, 1)
            ELSE DATEDIFF(day, sa.start_date, sa.effective_end_date)
        END AS mtbf_days,
        
        -- Mean Time To Repair (MTTR) in hours
        CASE 
            WHEN COALESCE(oi.unplanned_outages, 0) > 0 
            THEN COALESCE(oi.total_outage_minutes, 0) / 60.0 / COALESCE(oi.unplanned_outages, 1)
            ELSE 0
        END AS mttr_hours,
        
        -- Service utilization rate
        CASE 
            WHEN DATEDIFF(day, sa.start_date, sa.effective_end_date) > 0 
            THEN COALESCE(up.total_usage_days, 0) * 100.0 / DATEDIFF(day, sa.start_date, sa.effective_end_date)
            ELSE 0
        END AS utilization_rate,
        
        -- Reliability score (0-100)
        LEAST(100, GREATEST(0,
            (CASE 
                WHEN DATEDIFF(day, sa.start_date, sa.effective_end_date) >= 90 
                THEN 100.0 - (COALESCE(oi.outage_minutes_last_90_days, 0) / (90.0 * 24 * 60) * 100)
                ELSE 100.0 - (COALESCE(oi.total_outage_minutes, 0) / (DATEDIFF(day, sa.start_date, sa.effective_end_date) * 24.0 * 60) * 100)
            END) * 0.6 +  -- 60% weight on availability
            (CASE 
                WHEN COALESCE(si.total_support_tickets, 0) = 0 THEN 100
                WHEN COALESCE(si.critical_tickets, 0) = 0 THEN 90
                WHEN COALESCE(si.high_priority_tickets, 0) = 0 THEN 80
                ELSE GREATEST(50, 100 - (COALESCE(si.total_support_tickets, 0) * 5))
            END) * 0.25 +  -- 25% weight on support quality
            (CASE 
                WHEN COALESCE(up.total_usage_days, 0) = 0 THEN 50
                WHEN COALESCE(up.total_usage_days, 0) * 100.0 / DATEDIFF(day, sa.start_date, sa.effective_end_date) >= 80 THEN 100
                ELSE COALESCE(up.total_usage_days, 0) * 100.0 / DATEDIFF(day, sa.start_date, sa.effective_end_date) * 1.25
            END) * 0.15   -- 15% weight on utilization
        )) AS reliability_score
        
    FROM service_assignments_with_details sa
    LEFT JOIN outage_impact_by_assignment oi ON sa.assignment_id = oi.assignment_id
    LEFT JOIN usage_patterns up ON sa.assignment_id = up.assignment_id
    LEFT JOIN support_incidents si ON sa.assignment_id = si.assignment_id
)

SELECT 
    assignment_id,
    customer_key,
    service_key,
    customer_segment,
    customer_tier,
    service_type,
    service_name,
    monthly_cost,
    state,
    city,
    start_date,
    end_date,
    status,
    service_tenure_days,
    
    -- Outage metrics
    total_outages,
    unplanned_outages,
    planned_outages,
    total_outage_minutes,
    avg_outage_duration_minutes,
    max_outage_duration_minutes,
    outages_last_90_days,
    outage_minutes_last_90_days,
    weekend_outages,
    business_hours_outages,
    days_since_last_outage,
    
    -- Usage metrics
    total_usage_days,
    avg_daily_usage,
    usage_volatility,
    peak_usage,
    total_usage_cost,
    usage_days_last_30,
    avg_usage_last_30_days,
    months_with_usage,
    days_since_last_usage,
    
    -- Support metrics
    total_support_tickets,
    high_priority_tickets,
    critical_tickets,
    avg_resolution_hours,
    tickets_last_90_days,
    days_since_last_ticket,
    
    -- Calculated reliability metrics
    ROUND(availability_percentage, 2) AS availability_percentage,
    ROUND(mtbf_days, 1) AS mtbf_days,
    ROUND(mttr_hours, 2) AS mttr_hours,
    ROUND(utilization_rate, 2) AS utilization_rate,
    ROUND(reliability_score, 1) AS reliability_score,
    
    -- SLA compliance flags (assuming 99.5% availability SLA)
    CASE WHEN availability_percentage >= 99.5 THEN 1 ELSE 0 END AS meets_availability_sla,
    CASE WHEN mttr_hours <= 4 THEN 1 ELSE 0 END AS meets_mttr_sla,
    CASE WHEN reliability_score >= 95 THEN 1 ELSE 0 END AS meets_reliability_target,
    
    -- Risk indicators
    CASE 
        WHEN outages_last_90_days >= 5 OR availability_percentage < 95 THEN 'High Risk'
        WHEN outages_last_90_days >= 3 OR availability_percentage < 98 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS reliability_risk_level,
    
    CURRENT_TIMESTAMP() AS last_updated

FROM service_reliability_scores
WHERE service_tenure_days >= 30  -- Only include services with at least 30 days of history
ORDER BY reliability_score ASC, total_outages DESC