{{ config(
    materialized='table',
    tags=['customer_analytics', 'segmentation']
) }}

WITH customer_metrics AS (
    SELECT 
        customer_key,
        customer_id,
        customer_segment,
        customer_tier,
        status,
        total_revenue,
        revenue_last_12_months,
        total_services,
        active_services,
        total_monthly_cost,
        customer_tenure_days,
        total_tickets,
        tickets_last_6_months,
        is_churned,
        churn_risk_flag,
        total_data_consumed_12m,
        campaign_response_rate,
        service_utilization_rate
    FROM {{ ref('customer_360_view') }}
),

segment_stats AS (
    SELECT 
        customer_segment,
        customer_tier,
        COUNT(*) AS customer_count,
        SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) AS active_customers,
        SUM(CASE WHEN is_churned = 1 THEN 1 ELSE 0 END) AS churned_customers,
        SUM(CASE WHEN churn_risk_flag = 1 THEN 1 ELSE 0 END) AS at_risk_customers,
        
        -- Revenue metrics
        SUM(total_revenue) AS total_segment_revenue,
        SUM(revenue_last_12_months) AS revenue_last_12_months,
        AVG(total_revenue) AS avg_customer_revenue,
        AVG(revenue_last_12_months) AS avg_customer_revenue_12m,
        MEDIAN(total_revenue) AS median_customer_revenue,
        
        -- Service metrics
        AVG(total_services) AS avg_services_per_customer,
        AVG(active_services) AS avg_active_services,
        AVG(total_monthly_cost) AS avg_monthly_cost,
        SUM(total_monthly_cost) AS total_monthly_recurring_revenue,
        
        -- Engagement metrics
        AVG(customer_tenure_days) AS avg_tenure_days,
        AVG(total_tickets) AS avg_tickets_per_customer,
        AVG(tickets_last_6_months) AS avg_recent_tickets,
        AVG(total_data_consumed_12m) AS avg_data_consumption,
        AVG(campaign_response_rate) AS avg_campaign_response_rate,
        AVG(service_utilization_rate) AS avg_service_utilization
    FROM customer_metrics
    GROUP BY customer_segment, customer_tier
),

value_based_segments AS (
    SELECT 
        customer_key,
        customer_id,
        customer_segment,
        customer_tier,
        revenue_last_12_months,
        total_services,
        customer_tenure_days,
        is_churned,
        churn_risk_flag,
        
        -- Value-based segmentation
        CASE 
            WHEN revenue_last_12_months >= 2000 AND total_services >= 3 THEN 'High Value Multi-Service'
            WHEN revenue_last_12_months >= 2000 AND total_services < 3 THEN 'High Value Single-Service'
            WHEN revenue_last_12_months >= 1000 AND revenue_last_12_months < 2000 THEN 'Medium Value'
            WHEN revenue_last_12_months >= 500 AND revenue_last_12_months < 1000 THEN 'Low Value'
            WHEN revenue_last_12_months < 500 AND revenue_last_12_months > 0 THEN 'Minimal Value'
            ELSE 'No Recent Revenue'
        END AS value_segment,
        
        -- Behavioral segmentation
        CASE 
            WHEN is_churned = 1 THEN 'Churned'
            WHEN churn_risk_flag = 1 THEN 'At Risk'
            WHEN customer_tenure_days >= 365 AND revenue_last_12_months > 0 THEN 'Loyal'
            WHEN customer_tenure_days < 365 AND revenue_last_12_months > 0 THEN 'New'
            ELSE 'Inactive'
        END AS behavioral_segment,
        
        -- Lifecycle stage
        CASE 
            WHEN customer_tenure_days <= 90 THEN 'Onboarding'
            WHEN customer_tenure_days <= 365 THEN 'Growing'
            WHEN customer_tenure_days <= 1095 AND revenue_last_12_months > 0 THEN 'Mature'
            WHEN customer_tenure_days > 1095 AND revenue_last_12_months > 0 THEN 'Veteran'
            ELSE 'Dormant'
        END AS lifecycle_stage
    FROM customer_metrics
),

segment_performance AS (
    SELECT 
        value_segment,
        behavioral_segment,
        lifecycle_stage,
        COUNT(*) AS segment_size,
        SUM(revenue_last_12_months) AS total_revenue,
        AVG(revenue_last_12_months) AS avg_revenue,
        COUNT(CASE WHEN is_churned = 1 THEN 1 END) AS churned_count,
        COUNT(CASE WHEN churn_risk_flag = 1 THEN 1 END) AS at_risk_count,
        
        -- Churn rates
        CASE WHEN COUNT(*) > 0 
             THEN COUNT(CASE WHEN is_churned = 1 THEN 1 END) * 100.0 / COUNT(*)
             ELSE 0 END AS churn_rate,
        
        CASE WHEN COUNT(*) > 0 
             THEN COUNT(CASE WHEN churn_risk_flag = 1 THEN 1 END) * 100.0 / COUNT(*)
             ELSE 0 END AS at_risk_rate
    FROM value_based_segments
    GROUP BY value_segment, behavioral_segment, lifecycle_stage
)

SELECT 
    -- Traditional segmentation
    ss.customer_segment,
    ss.customer_tier,
    ss.customer_count,
    ss.active_customers,
    ss.churned_customers,
    ss.at_risk_customers,
    
    -- Churn rates by traditional segments
    CASE WHEN ss.customer_count > 0 
         THEN ss.churned_customers * 100.0 / ss.customer_count 
         ELSE 0 END AS churn_rate,
    
    CASE WHEN ss.customer_count > 0 
         THEN ss.at_risk_customers * 100.0 / ss.customer_count 
         ELSE 0 END AS at_risk_rate,
    
    -- Revenue metrics
    ss.total_segment_revenue,
    ss.revenue_last_12_months,
    ss.avg_customer_revenue,
    ss.avg_customer_revenue_12m,
    ss.median_customer_revenue,
    ss.total_monthly_recurring_revenue,
    
    -- Service and engagement metrics
    ss.avg_services_per_customer,
    ss.avg_active_services,
    ss.avg_monthly_cost,
    ss.avg_tenure_days,
    ss.avg_tickets_per_customer,
    ss.avg_recent_tickets,
    ss.avg_data_consumption,
    ss.avg_campaign_response_rate,
    ss.avg_service_utilization,
    
    -- Calculated insights
    CASE WHEN ss.customer_count > 0 
         THEN ss.revenue_last_12_months / ss.customer_count 
         ELSE 0 END AS revenue_per_customer,
    
    CASE WHEN ss.total_segment_revenue > 0 
         THEN ss.revenue_last_12_months * 100.0 / ss.total_segment_revenue 
         ELSE 0 END AS revenue_concentration_12m,
    
    -- Segment priority scoring (1-10)
    CASE 
        WHEN ss.avg_customer_revenue_12m >= 2000 AND (ss.churned_customers * 100.0 / ss.customer_count) <= 5 THEN 10
        WHEN ss.avg_customer_revenue_12m >= 1500 AND (ss.churned_customers * 100.0 / ss.customer_count) <= 10 THEN 9
        WHEN ss.avg_customer_revenue_12m >= 1000 AND (ss.churned_customers * 100.0 / ss.customer_count) <= 15 THEN 8
        WHEN ss.avg_customer_revenue_12m >= 1000 AND (ss.churned_customers * 100.0 / ss.customer_count) > 15 THEN 6
        WHEN ss.avg_customer_revenue_12m >= 500 AND (ss.churned_customers * 100.0 / ss.customer_count) <= 10 THEN 7
        WHEN ss.avg_customer_revenue_12m >= 500 AND (ss.churned_customers * 100.0 / ss.customer_count) > 10 THEN 5
        WHEN ss.avg_customer_revenue_12m < 500 AND (ss.churned_customers * 100.0 / ss.customer_count) <= 20 THEN 4
        ELSE 3
    END AS segment_priority_score,
    
    CURRENT_TIMESTAMP() AS analysis_date

FROM segment_stats ss
WHERE ss.customer_count > 0
ORDER BY ss.total_segment_revenue DESC, ss.avg_customer_revenue_12m DESC