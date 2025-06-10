--
-- Early Warning System Dashboard
-- 
-- Purpose: Operationalizes network churn risk scores into actionable alerts and interventions
-- Features: Risk alerts, intervention recommendations, customer contact prioritization
-- Business Value: Enables proactive customer retention and reduces reactive churn
--

{{ config(
    materialized='table',
    schema='kpis',
    alias='early_warning_system'
) }}

WITH current_risk_scores AS (
  SELECT 
    customer_id,
    analysis_month,
    network_churn_risk_score,
    risk_level,
    network_incident_score,
    support_ticket_score,
    usage_decline_score,
    service_quality_score,
    outage_count,
    unplanned_outages,
    network_ticket_count,
    current_usage_gb,
    prev_month_usage_gb
  FROM {{ ref('network_churn_risk_score') }}
  WHERE analysis_month = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()))
),

-- Historical trend analysis
risk_trends AS (
  SELECT 
    customer_id,
    network_churn_risk_score as current_score,
    LAG(network_churn_risk_score, 1) OVER (PARTITION BY customer_id ORDER BY analysis_month) as prev_month_score,
    LAG(network_churn_risk_score, 2) OVER (PARTITION BY customer_id ORDER BY analysis_month) as two_months_ago_score,
    CASE 
      WHEN LAG(network_churn_risk_score, 1) OVER (PARTITION BY customer_id ORDER BY analysis_month) IS NOT NULL THEN
        network_churn_risk_score - LAG(network_churn_risk_score, 1) OVER (PARTITION BY customer_id ORDER BY analysis_month)
      ELSE 0
    END as score_change_month_over_month
  FROM {{ ref('network_churn_risk_score') }}
  WHERE analysis_month >= DATE_TRUNC('month', DATEADD('month', -3, CURRENT_DATE()))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY analysis_month DESC) = 1
),

-- Customer profile enrichment
customer_context AS (
  SELECT 
    dc.customer_id,
    dc.customer_segment,
    dc.customer_tier,
    dc.account_balance,
    -- Calculate customer tenure in months
    DATEDIFF('month', dc.start_date, CURRENT_DATE()) as tenure_months,
    -- Recent invoice amount
    COALESCE(recent_revenue.monthly_revenue, 0) as monthly_revenue
  FROM {{ ref('silver_dim_customer') }} dc
  LEFT JOIN (
    SELECT 
      fi.customer_id,
      SUM(fi.total_amount) as monthly_revenue
    FROM {{ ref('silver_fact_billing') }} fi
    JOIN {{ ref('silver_dim_date') }} dd ON fi.invoice_date_key = dd.date_key
    WHERE dd.date_day >= DATEADD('month', -1, CURRENT_DATE())
    GROUP BY fi.customer_id
  ) recent_revenue ON dc.customer_id = recent_revenue.customer_id
  WHERE dc.status = 'Active'
),

-- Alert generation and prioritization
alert_engine AS (
  SELECT 
    crs.customer_id,
    crs.network_churn_risk_score,
    crs.risk_level,
    rt.score_change_month_over_month,
    cc.customer_segment,
    cc.customer_tier,
    cc.monthly_revenue,
    cc.tenure_months,
    
    -- Alert severity based on multiple factors
    CASE 
      WHEN crs.risk_level = 'Critical' AND rt.score_change_month_over_month > 10 THEN 'Immediate Action Required'
      WHEN crs.risk_level = 'Critical' THEN 'High Priority'
      WHEN crs.risk_level = 'High' AND rt.score_change_month_over_month > 15 THEN 'High Priority'
      WHEN crs.risk_level = 'High' AND cc.customer_tier IN ('Gold', 'Platinum') THEN 'Medium Priority'
      WHEN crs.risk_level = 'High' THEN 'Medium Priority'
      WHEN crs.risk_level = 'Medium' AND cc.monthly_revenue > 500 THEN 'Low Priority'
      ELSE 'Monitor'
    END as alert_severity,
    
    -- Intervention recommendations
    CASE 
      WHEN crs.network_incident_score >= 50 THEN 'Network Infrastructure Review'
      WHEN crs.support_ticket_score >= 40 THEN 'Dedicated Support Assignment'
      WHEN crs.usage_decline_score >= 30 THEN 'Service Usage Consultation'
      WHEN crs.service_quality_score >= 25 THEN 'Quality Assurance Check'
      ELSE 'General Account Review'
    END as primary_intervention,
    
    -- Secondary recommendations
    CASE 
      WHEN cc.customer_tier IN ('Gold', 'Platinum') THEN 'VIP Retention Program'
      WHEN cc.monthly_revenue > 300 THEN 'Account Manager Contact'
      WHEN cc.tenure_months > 24 THEN 'Loyalty Program Enrollment'
      WHEN crs.outage_count > 2 THEN 'Service Credit Offer'
      ELSE 'Standard Retention Process'
    END as secondary_intervention,
    
    -- Financial impact assessment
    CASE 
      WHEN cc.monthly_revenue > 0 THEN cc.monthly_revenue * 12 * 0.8  -- Estimated annual value with retention probability
      ELSE 200 * 12 * 0.8  -- Default ARPU estimate
    END as potential_revenue_at_risk,
    
    -- Contact priority score (1-100)
    ROUND(
      (crs.network_churn_risk_score * 0.4) +
      (CASE cc.customer_tier 
         WHEN 'Platinum' THEN 40
         WHEN 'Gold' THEN 30
         WHEN 'Silver' THEN 20
         ELSE 10
       END * 0.2) +
      (LEAST(50, cc.monthly_revenue / 10) * 0.2) +
      (GREATEST(0, rt.score_change_month_over_month) * 0.2),
      0
    ) as contact_priority_score,
    
    -- Component details for action planning
    crs.outage_count,
    crs.unplanned_outages,
    crs.network_ticket_count,
    crs.current_usage_gb,
    crs.prev_month_usage_gb,
    
    -- Timing recommendations
    CASE 
      WHEN crs.risk_level = 'Critical' THEN 'Within 24 hours'
      WHEN crs.risk_level = 'High' THEN 'Within 3 days'
      WHEN crs.risk_level = 'Medium' THEN 'Within 1 week'
      ELSE 'Within 2 weeks'
    END as recommended_contact_timeframe

  FROM current_risk_scores crs
  JOIN risk_trends rt ON crs.customer_id = rt.customer_id
  JOIN customer_context cc ON crs.customer_id = cc.customer_id
  WHERE crs.risk_level IN ('Critical', 'High', 'Medium')
)

SELECT 
  customer_id,
  network_churn_risk_score,
  risk_level,
  alert_severity,
  contact_priority_score,
  primary_intervention,
  secondary_intervention,
  recommended_contact_timeframe,
  potential_revenue_at_risk,
  customer_segment,
  customer_tier,
  monthly_revenue,
  tenure_months,
  score_change_month_over_month,
  outage_count,
  unplanned_outages,
  network_ticket_count,
  current_usage_gb,
  prev_month_usage_gb,
  
  -- Alert generated timestamp
  CURRENT_TIMESTAMP() as alert_generated_at,
  
  -- Target contact date
  CASE 
    WHEN recommended_contact_timeframe = 'Within 24 hours' THEN DATEADD('day', 1, CURRENT_DATE())
    WHEN recommended_contact_timeframe = 'Within 3 days' THEN DATEADD('day', 3, CURRENT_DATE())
    WHEN recommended_contact_timeframe = 'Within 1 week' THEN DATEADD('day', 7, CURRENT_DATE())
    ELSE DATEADD('day', 14, CURRENT_DATE())
  END as target_contact_date

FROM alert_engine
ORDER BY contact_priority_score DESC, potential_revenue_at_risk DESC