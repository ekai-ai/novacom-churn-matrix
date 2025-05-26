{{ config(
    materialized='table',
    tags=['network_ops', 'root_cause_analysis', 'outage_investigation']
) }}

WITH outage_details AS (
    SELECT 
        no.outage_id,
        no.assignment_id,
        no.outage_type,
        no.region,
        no.cause,
        no.resolution,
        no.impacted_customers_count,
        ds.full_date AS start_date,
        de.full_date AS end_date,
        ds.year AS outage_year,
        ds.quarter AS outage_quarter,
        ds.month AS outage_month,
        ds.day_of_week AS outage_day_of_week,
        ds.hour AS outage_hour,
        DATEDIFF(minute, ds.full_date, de.full_date) AS duration_minutes,
        DATEDIFF(hour, ds.full_date, de.full_date) AS duration_hours
    FROM {{ source('dimensional_warehouse', 'factnetworkoutage') }} no
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} ds ON no.start_date_key = ds.date_key
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} de ON no.end_date_key = de.date_key
    WHERE ds.full_date >= DATEADD(month, -24, CURRENT_DATE())
),

service_context AS (
    SELECT 
        sa.assignment_id,
        sa.customer_key,
        sa.service_key,
        c.customer_segment,
        c.customer_tier,
        c.state,
        c.city,
        s.service_type,
        s.service_name,
        s.monthly_cost
    FROM {{ source('dimensional_warehouse', 'factserviceassignment') }} sa
    JOIN {{ source('dimensional_warehouse', 'dimcustomer') }} c ON sa.customer_key = c.customer_key
    JOIN {{ source('dimensional_warehouse', 'dimservice') }} s ON sa.service_key = s.service_key
),

cause_analysis AS (
    SELECT 
        od.cause,
        od.outage_type,
        od.region,
        
        -- Basic metrics
        COUNT(DISTINCT od.outage_id) AS total_outages,
        SUM(od.duration_minutes) AS total_duration_minutes,
        AVG(od.duration_minutes) AS avg_duration_minutes,
        MEDIAN(od.duration_minutes) AS median_duration_minutes,
        MAX(od.duration_minutes) AS max_duration_minutes,
        MIN(od.duration_minutes) AS min_duration_minutes,
        
        -- Customer impact
        SUM(od.impacted_customers_count) AS total_customers_impacted,
        AVG(od.impacted_customers_count) AS avg_customers_per_outage,
        MAX(od.impacted_customers_count) AS max_customers_in_single_outage,
        
        -- Temporal patterns
        COUNT(CASE WHEN od.outage_day_of_week IN (1, 7) THEN od.outage_id END) AS weekend_outages,
        COUNT(CASE WHEN od.outage_hour BETWEEN 9 AND 17 THEN od.outage_id END) AS business_hours_outages,
        COUNT(CASE WHEN od.outage_hour BETWEEN 18 AND 23 OR od.outage_hour BETWEEN 0 AND 6 THEN od.outage_id END) AS off_hours_outages,
        
        -- Seasonal patterns
        COUNT(CASE WHEN od.outage_month IN (12, 1, 2) THEN od.outage_id END) AS winter_outages,
        COUNT(CASE WHEN od.outage_month IN (3, 4, 5) THEN od.outage_id END) AS spring_outages,
        COUNT(CASE WHEN od.outage_month IN (6, 7, 8) THEN od.outage_id END) AS summer_outages,
        COUNT(CASE WHEN od.outage_month IN (9, 10, 11) THEN od.outage_id END) AS fall_outages,
        
        -- Recent trends (last 6 months vs previous 6 months)
        COUNT(CASE WHEN od.start_date >= DATEADD(month, -6, CURRENT_DATE()) THEN od.outage_id END) AS outages_last_6_months,
        COUNT(CASE WHEN od.start_date >= DATEADD(month, -12, CURRENT_DATE()) 
                   AND od.start_date < DATEADD(month, -6, CURRENT_DATE()) THEN od.outage_id END) AS outages_prev_6_months,
        
        -- Severity classification
        COUNT(CASE WHEN od.duration_minutes <= 60 THEN od.outage_id END) AS short_outages_1h,
        COUNT(CASE WHEN od.duration_minutes BETWEEN 61 AND 240 THEN od.outage_id END) AS medium_outages_1_4h,
        COUNT(CASE WHEN od.duration_minutes BETWEEN 241 AND 480 THEN od.outage_id END) AS long_outages_4_8h,
        COUNT(CASE WHEN od.duration_minutes > 480 THEN od.outage_id END) AS extended_outages_8h_plus,
        
        -- Most recent occurrence
        MAX(od.start_date) AS most_recent_outage_date,
        DATEDIFF(day, MAX(od.start_date), CURRENT_DATE()) AS days_since_last_occurrence
        
    FROM outage_details od
    GROUP BY od.cause, od.outage_type, od.region
),

service_impact_analysis AS (
    SELECT 
        od.cause,
        sc.service_type,
        sc.customer_segment,
        
        -- Service-specific impact
        COUNT(DISTINCT od.outage_id) AS outages_affecting_service,
        COUNT(DISTINCT od.assignment_id) AS unique_assignments_affected,
        COUNT(DISTINCT sc.customer_key) AS unique_customers_affected,
        SUM(od.duration_minutes) AS total_service_downtime_minutes,
        AVG(od.duration_minutes) AS avg_outage_duration_minutes,
        
        -- Revenue impact estimation
        SUM(sc.monthly_cost * (od.duration_minutes / (30.0 * 24 * 60))) AS estimated_revenue_impact,
        AVG(sc.monthly_cost) AS avg_affected_service_value,
        
        -- Customer tier impact
        COUNT(CASE WHEN sc.customer_tier = 'Platinum' THEN od.outage_id END) AS platinum_customer_outages,
        COUNT(CASE WHEN sc.customer_tier = 'Gold' THEN od.outage_id END) AS gold_customer_outages,
        COUNT(CASE WHEN sc.customer_tier = 'Silver' THEN od.outage_id END) AS silver_customer_outages,
        COUNT(CASE WHEN sc.customer_tier = 'Bronze' THEN od.outage_id END) AS bronze_customer_outages
        
    FROM outage_details od
    JOIN service_context sc ON od.assignment_id = sc.assignment_id
    GROUP BY od.cause, sc.service_type, sc.customer_segment
),

resolution_effectiveness AS (
    SELECT 
        od.cause,
        od.resolution,
        
        -- Resolution metrics
        COUNT(DISTINCT od.outage_id) AS outages_with_resolution,
        AVG(od.duration_minutes) AS avg_resolution_time_minutes,
        
        -- Recurrence analysis (outages with same cause within 30 days)
        COUNT(CASE WHEN EXISTS (
            SELECT 1 FROM outage_details od2 
            WHERE od2.cause = od.cause 
            AND od2.region = od.region 
            AND od2.start_date BETWEEN od.end_date AND DATEADD(day, 30, od.end_date)
            AND od2.outage_id != od.outage_id
        ) THEN od.outage_id END) AS recurring_within_30_days,
        
        -- Resolution quality score
        CASE 
            WHEN AVG(od.duration_minutes) <= 60 THEN 'Excellent'
            WHEN AVG(od.duration_minutes) <= 240 THEN 'Good'
            WHEN AVG(od.duration_minutes) <= 480 THEN 'Fair'
            ELSE 'Poor'
        END AS resolution_quality
        
    FROM outage_details od
    WHERE od.resolution IS NOT NULL AND od.resolution != ''
    GROUP BY od.cause, od.resolution
),

predictive_patterns AS (
    SELECT 
        od.cause,
        od.region,
        
        -- Pattern identification
        COUNT(DISTINCT od.outage_id) AS total_occurrences,
        
        -- Time-based patterns
        CASE 
            WHEN COUNT(CASE WHEN od.outage_day_of_week IN (1, 7) THEN od.outage_id END) * 100.0 / COUNT(od.outage_id) > 40 
            THEN 'Weekend Pattern'
            WHEN COUNT(CASE WHEN od.outage_hour BETWEEN 9 AND 17 THEN od.outage_id END) * 100.0 / COUNT(od.outage_id) > 60 
            THEN 'Business Hours Pattern'
            WHEN COUNT(CASE WHEN od.outage_month IN (6, 7, 8) THEN od.outage_id END) * 100.0 / COUNT(od.outage_id) > 50 
            THEN 'Summer Pattern'
            WHEN COUNT(CASE WHEN od.outage_month IN (12, 1, 2) THEN od.outage_id END) * 100.0 / COUNT(od.outage_id) > 50 
            THEN 'Winter Pattern'
            ELSE 'No Clear Pattern'
        END AS temporal_pattern,
        
        -- Frequency analysis
        CASE 
            WHEN COUNT(od.outage_id) >= 10 THEN 'High Frequency'
            WHEN COUNT(od.outage_id) >= 5 THEN 'Medium Frequency'
            WHEN COUNT(od.outage_id) >= 2 THEN 'Low Frequency'
            ELSE 'Rare'
        END AS frequency_category,
        
        -- Trend analysis
        CASE 
            WHEN COUNT(CASE WHEN od.start_date >= DATEADD(month, -6, CURRENT_DATE()) THEN od.outage_id END) > 
                 COUNT(CASE WHEN od.start_date >= DATEADD(month, -12, CURRENT_DATE()) 
                            AND od.start_date < DATEADD(month, -6, CURRENT_DATE()) THEN od.outage_id END) 
            THEN 'Increasing'
            WHEN COUNT(CASE WHEN od.start_date >= DATEADD(month, -6, CURRENT_DATE()) THEN od.outage_id END) < 
                 COUNT(CASE WHEN od.start_date >= DATEADD(month, -12, CURRENT_DATE()) 
                            AND od.start_date < DATEADD(month, -6, CURRENT_DATE()) THEN od.outage_id END) 
            THEN 'Decreasing'
            ELSE 'Stable'
        END AS trend_direction
        
    FROM outage_details od
    GROUP BY od.cause, od.region
    HAVING COUNT(DISTINCT od.outage_id) >= 2
)

SELECT 
    ca.cause,
    ca.outage_type,
    ca.region,
    
    -- Basic metrics
    ca.total_outages,
    ca.total_duration_minutes,
    ROUND(ca.avg_duration_minutes, 1) AS avg_duration_minutes,
    ca.median_duration_minutes,
    ca.max_duration_minutes,
    ca.min_duration_minutes,
    
    -- Customer impact
    ca.total_customers_impacted,
    ROUND(ca.avg_customers_per_outage, 1) AS avg_customers_per_outage,
    ca.max_customers_in_single_outage,
    
    -- Temporal analysis
    ca.weekend_outages,
    ca.business_hours_outages,
    ca.off_hours_outages,
    ca.winter_outages,
    ca.spring_outages,
    ca.summer_outages,
    ca.fall_outages,
    
    -- Trend analysis
    ca.outages_last_6_months,
    ca.outages_prev_6_months,
    CASE 
        WHEN ca.outages_prev_6_months > 0 
        THEN ROUND((ca.outages_last_6_months - ca.outages_prev_6_months) * 100.0 / ca.outages_prev_6_months, 1)
        ELSE NULL 
    END AS trend_percentage_change,
    
    -- Severity distribution
    ca.short_outages_1h,
    ca.medium_outages_1_4h,
    ca.long_outages_4_8h,
    ca.extended_outages_8h_plus,
    
    -- Recency
    ca.most_recent_outage_date,
    ca.days_since_last_occurrence,
    
    -- Pattern insights
    pp.temporal_pattern,
    pp.frequency_category,
    pp.trend_direction,
    
    -- Priority scoring (1-10, higher = more critical)
    LEAST(10, GREATEST(1, 
        (ca.total_outages * 0.2) +  -- Frequency weight
        (ca.total_customers_impacted * 0.0001) +  -- Customer impact weight
        (CASE ca.outage_type WHEN 'Unplanned' THEN 3 ELSE 1 END) +  -- Type weight
        (CASE WHEN ca.avg_duration_minutes > 240 THEN 2 ELSE 0 END) +  -- Duration weight
        (CASE WHEN ca.outages_last_6_months > ca.outages_prev_6_months THEN 2 ELSE 0 END)  -- Trend weight
    )) AS priority_score,
    
    -- Recommendations
    CASE 
        WHEN ca.cause = 'Equipment Failure' AND ca.outages_last_6_months > ca.outages_prev_6_months 
        THEN 'Equipment replacement/maintenance program needed'
        WHEN ca.cause = 'Weather' AND ca.summer_outages > ca.winter_outages * 2 
        THEN 'Weather resilience improvements for summer conditions'
        WHEN ca.cause = 'Power Outage' AND ca.total_outages >= 5 
        THEN 'Backup power solutions evaluation'
        WHEN ca.business_hours_outages * 100.0 / ca.total_outages > 60 
        THEN 'Preventive maintenance scheduling optimization'
        WHEN pp.frequency_category = 'High Frequency' 
        THEN 'Root cause investigation and systematic fix required'
        ELSE 'Continue monitoring'
    END AS recommended_action,
    
    CURRENT_TIMESTAMP() AS analysis_date

FROM cause_analysis ca
LEFT JOIN predictive_patterns pp ON ca.cause = pp.cause AND ca.region = pp.region
WHERE ca.total_outages >= 1
ORDER BY 
    LEAST(10, GREATEST(1, 
        (ca.total_outages * 0.2) +
        (ca.total_customers_impacted * 0.0001) +
        (CASE ca.outage_type WHEN 'Unplanned' THEN 3 ELSE 1 END) +
        (CASE WHEN ca.avg_duration_minutes > 240 THEN 2 ELSE 0 END) +
        (CASE WHEN ca.outages_last_6_months > ca.outages_prev_6_months THEN 2 ELSE 0 END)
    )) DESC,
    ca.total_customers_impacted DESC,
    ca.total_outages DESC