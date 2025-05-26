{{ config(
    materialized='table',
    tags=['network_ops', 'performance_monitoring']
) }}

WITH network_outages AS (
    SELECT 
        no.outage_id,
        no.assignment_id,
        no.outage_type,
        no.region,
        no.impacted_customers_count,
        no.cause,
        no.resolution,
        ds.full_date AS start_date,
        de.full_date AS end_date,
        DATEDIFF(hour, ds.full_date, de.full_date) AS outage_duration_hours,
        DATEDIFF(minute, ds.full_date, de.full_date) AS outage_duration_minutes,
        ds.year AS outage_year,
        ds.quarter AS outage_quarter,
        ds.month AS outage_month,
        ds.day_of_week AS outage_day_of_week
    FROM {{ source('dimensional_warehouse', 'factnetworkoutage') }} no
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} ds ON no.start_date_key = ds.date_key
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} de ON no.end_date_key = de.date_key
    WHERE ds.full_date >= DATEADD(month, -24, CURRENT_DATE())
),

network_usage AS (
    SELECT 
        nu.assignment_id,
        d.full_date AS usage_date,
        nu.usage_type,
        nu.data_consumed,
        nu.usage_unit,
        nu.usage_peak,
        nu.usage_cost,
        d.year AS usage_year,
        d.quarter AS usage_quarter,
        d.month AS usage_month,
        d.day_of_week AS usage_day_of_week
    FROM {{ source('dimensional_warehouse', 'factnetworkusage') }} nu
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} d ON nu.usage_date_key = d.date_key
    WHERE d.full_date >= DATEADD(month, -24, CURRENT_DATE())
),

service_assignments AS (
    SELECT 
        sa.assignment_id,
        sa.customer_key,
        sa.service_key,
        sa.status AS assignment_status,
        c.customer_segment,
        c.customer_tier,
        c.city,
        c.state,
        c.country,
        s.service_type,
        s.service_name,
        s.monthly_cost,
        ds.full_date AS start_date,
        de.full_date AS end_date
    FROM {{ source('dimensional_warehouse', 'factserviceassignment') }} sa
    JOIN {{ source('dimensional_warehouse', 'dimcustomer') }} c ON sa.customer_key = c.customer_key
    JOIN {{ source('dimensional_warehouse', 'dimservice') }} s ON sa.service_key = s.service_key
    JOIN {{ source('dimensional_warehouse', 'dim_date') }} ds ON sa.start_date_key = ds.date_key
    LEFT JOIN {{ source('dimensional_warehouse', 'dim_date') }} de ON sa.end_date_key = de.date_key
),

regional_performance AS (
    SELECT 
        COALESCE(sa.state, no.region, 'Unknown') AS region,
        
        -- Outage metrics
        COUNT(DISTINCT no.outage_id) AS total_outages,
        COUNT(DISTINCT CASE WHEN no.outage_type = 'Unplanned' THEN no.outage_id END) AS unplanned_outages,
        COUNT(DISTINCT CASE WHEN no.outage_type = 'Planned' THEN no.outage_id END) AS planned_outages,
        SUM(no.outage_duration_hours) AS total_outage_hours,
        AVG(no.outage_duration_hours) AS avg_outage_duration_hours,
        SUM(no.impacted_customers_count) AS total_customers_impacted,
        AVG(no.impacted_customers_count) AS avg_customers_per_outage,
        
        -- Recent performance (last 30 days)
        COUNT(CASE WHEN no.start_date >= DATEADD(day, -30, CURRENT_DATE()) THEN no.outage_id END) AS outages_last_30_days,
        SUM(CASE WHEN no.start_date >= DATEADD(day, -30, CURRENT_DATE()) THEN no.outage_duration_hours ELSE 0 END) AS outage_hours_last_30_days,
        
        -- Service metrics
        COUNT(DISTINCT sa.assignment_id) AS total_service_assignments,
        COUNT(DISTINCT CASE WHEN sa.assignment_status = 'Active' THEN sa.assignment_id END) AS active_assignments,
        COUNT(DISTINCT sa.customer_key) AS unique_customers_in_region,
        
        -- Usage metrics (last 30 days)
        AVG(CASE WHEN nu.usage_date >= DATEADD(day, -30, CURRENT_DATE()) THEN nu.data_consumed END) AS avg_daily_usage_30d,
        SUM(CASE WHEN nu.usage_date >= DATEADD(day, -30, CURRENT_DATE()) THEN nu.data_consumed ELSE 0 END) AS total_usage_30d,
        MAX(CASE WHEN nu.usage_date >= DATEADD(day, -30, CURRENT_DATE()) THEN nu.usage_peak END) AS peak_usage_30d
        
    FROM network_outages no
    FULL OUTER JOIN service_assignments sa ON no.assignment_id = sa.assignment_id
    LEFT JOIN network_usage nu ON sa.assignment_id = nu.assignment_id
    GROUP BY COALESCE(sa.state, no.region, 'Unknown')
),

service_type_performance AS (
    SELECT 
        sa.service_type,
        
        -- Outage impact by service type
        COUNT(DISTINCT no.outage_id) AS outages_affecting_service_type,
        AVG(no.outage_duration_hours) AS avg_outage_duration_hours,
        SUM(no.impacted_customers_count) AS customers_impacted_by_outages,
        
        -- Service metrics
        COUNT(DISTINCT sa.assignment_id) AS total_assignments,
        COUNT(DISTINCT CASE WHEN sa.assignment_status = 'Active' THEN sa.assignment_id END) AS active_assignments,
        COUNT(DISTINCT sa.customer_key) AS unique_customers,
        AVG(sa.monthly_cost) AS avg_monthly_cost,
        
        -- Usage patterns (last 90 days)
        AVG(CASE WHEN nu.usage_date >= DATEADD(day, -90, CURRENT_DATE()) THEN nu.data_consumed END) AS avg_daily_usage_90d,
        COUNT(DISTINCT CASE WHEN nu.usage_date >= DATEADD(day, -90, CURRENT_DATE()) THEN nu.assignment_id END) AS assignments_with_usage_90d,
        AVG(CASE WHEN nu.usage_date >= DATEADD(day, -90, CURRENT_DATE()) THEN nu.usage_cost END) AS avg_daily_usage_cost_90d
        
    FROM service_assignments sa
    LEFT JOIN network_outages no ON sa.assignment_id = no.assignment_id
    LEFT JOIN network_usage nu ON sa.assignment_id = nu.assignment_id
    GROUP BY sa.service_type
),

monthly_trends AS (
    SELECT 
        no.outage_year,
        no.outage_month,
        
        -- Monthly outage metrics
        COUNT(DISTINCT no.outage_id) AS monthly_outages,
        COUNT(DISTINCT CASE WHEN no.outage_type = 'Unplanned' THEN no.outage_id END) AS unplanned_outages,
        SUM(no.outage_duration_hours) AS total_outage_hours,
        AVG(no.outage_duration_hours) AS avg_outage_duration,
        SUM(no.impacted_customers_count) AS customers_impacted,
        
        -- Availability calculation (assuming 720 hours per month as baseline)
        100.0 - (SUM(no.outage_duration_hours) / 720.0 * 100) AS network_availability_percentage,
        
        -- Most common outage causes
        COUNT(CASE WHEN no.cause = 'Equipment Failure' THEN no.outage_id END) AS equipment_failure_outages,
        COUNT(CASE WHEN no.cause = 'Weather' THEN no.outage_id END) AS weather_related_outages,
        COUNT(CASE WHEN no.cause = 'Maintenance' THEN no.outage_id END) AS maintenance_outages,
        COUNT(CASE WHEN no.cause = 'Power Outage' THEN no.outage_id END) AS power_outages,
        
        -- Usage trends for the same month
        AVG(nu.data_consumed) AS avg_monthly_usage_per_assignment,
        COUNT(DISTINCT nu.assignment_id) AS assignments_with_usage
        
    FROM network_outages no
    LEFT JOIN network_usage nu ON no.outage_month = nu.usage_month 
                               AND no.outage_year = nu.usage_year
    GROUP BY no.outage_year, no.outage_month
),

critical_metrics AS (
    SELECT 
        -- Overall network health (last 30 days)
        COUNT(DISTINCT CASE WHEN no.start_date >= DATEADD(day, -30, CURRENT_DATE()) THEN no.outage_id END) AS outages_last_30_days,
        AVG(CASE WHEN no.start_date >= DATEADD(day, -30, CURRENT_DATE()) THEN no.outage_duration_hours END) AS avg_outage_duration_30d,
        COUNT(DISTINCT CASE WHEN no.start_date >= DATEADD(day, -30, CURRENT_DATE()) 
                            AND no.outage_type = 'Unplanned' THEN no.outage_id END) AS unplanned_outages_30d,
        
        -- Customer impact (last 30 days)
        SUM(CASE WHEN no.start_date >= DATEADD(day, -30, CURRENT_DATE()) THEN no.impacted_customers_count ELSE 0 END) AS customers_impacted_30d,
        COUNT(DISTINCT CASE WHEN no.start_date >= DATEADD(day, -30, CURRENT_DATE()) THEN no.region END) AS regions_affected_30d,
        
        -- Usage trends (last 30 days vs previous 30 days)
        AVG(CASE WHEN nu.usage_date >= DATEADD(day, -30, CURRENT_DATE()) THEN nu.data_consumed END) AS avg_usage_last_30d,
        AVG(CASE WHEN nu.usage_date >= DATEADD(day, -60, CURRENT_DATE()) 
                 AND nu.usage_date < DATEADD(day, -30, CURRENT_DATE()) THEN nu.data_consumed END) AS avg_usage_prev_30d,
        
        -- Peak usage analysis
        MAX(CASE WHEN nu.usage_date >= DATEADD(day, -30, CURRENT_DATE()) THEN nu.usage_peak END) AS peak_usage_30d,
        
        -- Service availability
        100.0 - (SUM(CASE WHEN no.start_date >= DATEADD(day, -30, CURRENT_DATE()) THEN no.outage_duration_hours ELSE 0 END) / (24.0 * 30) * 100) AS availability_percentage_30d
        
    FROM network_outages no
    FULL OUTER JOIN network_usage nu ON 1=1 
)

SELECT 
    'Regional Performance' AS metric_category,
    region,
    NULL AS service_type,
    NULL AS year,
    NULL AS month,
    total_outages,
    unplanned_outages,
    planned_outages,
    total_outage_hours,
    avg_outage_duration_hours,
    total_customers_impacted AS customers_impacted,
    avg_customers_per_outage,
    outages_last_30_days,
    outage_hours_last_30_days,
    total_service_assignments,
    active_assignments,
    unique_customers_in_region AS unique_customers,
    avg_daily_usage_30d,
    total_usage_30d,
    peak_usage_30d,
    NULL AS avg_monthly_cost,
    NULL AS network_availability_percentage,
    CURRENT_TIMESTAMP() AS last_updated
FROM regional_performance

UNION ALL

SELECT 
    'Service Type Performance' AS metric_category,
    NULL AS region,
    service_type,
    NULL AS year,
    NULL AS month,
    outages_affecting_service_type AS total_outages,
    NULL AS unplanned_outages,
    NULL AS planned_outages,
    NULL AS total_outage_hours,
    avg_outage_duration_hours,
    customers_impacted_by_outages AS customers_impacted,
    NULL AS avg_customers_per_outage,
    NULL AS outages_last_30_days,
    NULL AS outage_hours_last_30_days,
    total_assignments AS total_service_assignments,
    active_assignments,
    unique_customers,
    avg_daily_usage_90d AS avg_daily_usage_30d,
    NULL AS total_usage_30d,
    NULL AS peak_usage_30d,
    avg_monthly_cost,
    NULL AS network_availability_percentage,
    CURRENT_TIMESTAMP() AS last_updated
FROM service_type_performance

UNION ALL

SELECT 
    'Monthly Trends' AS metric_category,
    NULL AS region,
    NULL AS service_type,
    outage_year AS year,
    outage_month AS month,
    monthly_outages AS total_outages,
    unplanned_outages,
    NULL AS planned_outages,
    total_outage_hours,
    avg_outage_duration AS avg_outage_duration_hours,
    customers_impacted,
    NULL AS avg_customers_per_outage,
    NULL AS outages_last_30_days,
    NULL AS outage_hours_last_30_days,
    NULL AS total_service_assignments,
    NULL AS active_assignments,
    assignments_with_usage AS unique_customers,
    avg_monthly_usage_per_assignment AS avg_daily_usage_30d,
    NULL AS total_usage_30d,
    NULL AS peak_usage_30d,
    NULL AS avg_monthly_cost,
    network_availability_percentage,
    CURRENT_TIMESTAMP() AS last_updated
FROM monthly_trends

ORDER BY metric_category, region, service_type, year DESC, month DESC