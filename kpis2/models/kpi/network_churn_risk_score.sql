--
-- Network-Driven Churn Risk Score (NCRS)
-- 
-- Purpose: Predicts customer churn risk based on network performance issues
-- Calculation: Weighted score combining network incidents (40%), support tickets (30%), 
--              usage pattern changes (20%), and service quality metrics (10%)
-- Business Value: Enables proactive retention before customers decide to leave
--

{{ config(
    materialized='table',
    schema='kpis',
    alias='network_churn_risk_score'
) }}

WITH date_spine AS (
  SELECT DISTINCT 
    DATE_TRUNC('month', usage_date_key) as analysis_month
  FROM {{ source('Combined Source', 'nwk_usage') }}
  WHERE usage_date_key >= DATEADD('month', -6, CURRENT_DATE())
),

-- Network incidents affecting customers (40% weight)
network_incidents AS (
  SELECT 
    sa.customer_id,
    DATE_TRUNC('month', no.start_time_key) as analysis_month,
    COUNT(no.outage_id) as outage_count,
    SUM(CASE WHEN no.outage_type = 'Unplanned' THEN 1 ELSE 0 END) as unplanned_outages,
    AVG(DATEDIFF('hour', no.start_time_key, no.end_time_key)) as avg_outage_duration_hours,
    -- Network incident score (0-100)
    LEAST(100, 
      (COUNT(no.outage_id) * 15) + 
      (SUM(CASE WHEN no.outage_type = 'Unplanned' THEN 1 ELSE 0 END) * 25) +
      (AVG(DATEDIFF('hour', no.start_time_key, no.end_time_key)) * 2)
    ) as network_incident_score
  FROM {{ ref('FactServiceAssignment') }} sa
  JOIN {{ ref('FactNetworkOutage') }} no ON sa.assignment_id = no.assignment_id
  WHERE no.start_time_key >= DATEADD('month', -6, CURRENT_DATE())
  GROUP BY sa.customer_id, DATE_TRUNC('month', no.start_time_key)
),

-- Support tickets related to network issues (30% weight)
network_support_tickets AS (
  SELECT 
    st.customer_id,
    DATE_TRUNC('month', dd.date_day) as analysis_month,
    COUNT(st.ticket_id) as network_ticket_count,
    AVG(st.timetoResolutionSeconds / 3600.0) as avg_resolution_hours,
    -- Support ticket score (0-100)
    LEAST(100,
      (COUNT(st.ticket_id) * 20) +
      (AVG(st.timetoResolutionSeconds / 3600.0) * 1.5)
    ) as support_ticket_score
  FROM {{ ref('FactSupportTicket') }} st
  JOIN {{ ref('dim_date') }} dd ON st.created_date_key = dd.date_key
  JOIN {{ source('Combined Source', 'sup_tickets') }} st_raw ON st.ticket_id = st_raw.ticket_id
  WHERE dd.date_day >= DATEADD('month', -6, CURRENT_DATE())
    AND (LOWER(st_raw.subject) LIKE '%network%' 
         OR LOWER(st_raw.subject) LIKE '%outage%' 
         OR LOWER(st_raw.subject) LIKE '%connection%'
         OR LOWER(st_raw.subject) LIKE '%speed%'
         OR LOWER(st_raw.subject) LIKE '%service%')
  GROUP BY st.customer_id, DATE_TRUNC('month', dd.date_day)
),

-- Usage pattern changes (20% weight)
usage_patterns AS (
  SELECT 
    sa.customer_id,
    DATE_TRUNC('month', nu.usage_date_key) as analysis_month,
    SUM(nu.data_consumed) as monthly_usage_gb,
    LAG(SUM(nu.data_consumed), 1) OVER (PARTITION BY sa.customer_id ORDER BY DATE_TRUNC('month', nu.usage_date_key)) as prev_month_usage_gb,
    LAG(SUM(nu.data_consumed), 2) OVER (PARTITION BY sa.customer_id ORDER BY DATE_TRUNC('month', nu.usage_date_key)) as two_months_ago_usage_gb,
    -- Usage decline score (0-100)
    CASE 
      WHEN LAG(SUM(nu.data_consumed), 1) OVER (PARTITION BY sa.customer_id ORDER BY DATE_TRUNC('month', nu.usage_date_key)) > 0 THEN
        LEAST(100, GREATEST(0,
          ((LAG(SUM(nu.data_consumed), 1) OVER (PARTITION BY sa.customer_id ORDER BY DATE_TRUNC('month', nu.usage_date_key)) - 
            SUM(nu.data_consumed)) / 
           NULLIF(LAG(SUM(nu.data_consumed), 1) OVER (PARTITION BY sa.customer_id ORDER BY DATE_TRUNC('month', nu.usage_date_key)), 0)) * 200
        ))
      ELSE 0
    END as usage_decline_score
  FROM {{ ref('FactServiceAssignment') }} sa
  JOIN {{ ref('FactNetworkUsage') }} nu ON sa.assignment_id = nu.assignment_id
  WHERE nu.usage_date_key >= DATEADD('month', -6, CURRENT_DATE())
    AND nu.usage_type = 'Total'
  GROUP BY sa.customer_id, DATE_TRUNC('month', nu.usage_date_key)
),

-- Service quality metrics (10% weight)
service_quality AS (
  SELECT 
    sa.customer_id,
    DATE_TRUNC('month', nu.usage_date_key) as analysis_month,
    AVG(nu.usage_peak) as avg_peak_usage,
    COUNT(CASE WHEN nu.usage_peak > (SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY usage_peak) FROM {{ ref('FactNetworkUsage') }}) THEN 1 END) as peak_overload_incidents,
    -- Service quality score (0-100)
    LEAST(100,
      (COUNT(CASE WHEN nu.usage_peak > (SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY usage_peak) FROM {{ ref('FactNetworkUsage') }}) THEN 1 END) * 30)
    ) as service_quality_score
  FROM {{ ref('FactServiceAssignment') }} sa
  JOIN {{ ref('FactNetworkUsage') }} nu ON sa.assignment_id = nu.assignment_id
  WHERE nu.usage_date_key >= DATEADD('month', -6, CURRENT_DATE())
  GROUP BY sa.customer_id, DATE_TRUNC('month', nu.usage_date_key)
),

-- Customer base for all active customers
customer_base AS (
  SELECT DISTINCT 
    dc.customer_id,
    ds.analysis_month
  FROM {{ ref('DimCustomer') }} dc
  CROSS JOIN date_spine ds
  WHERE dc.status = 'Active'
),

-- Combine all components
final_scores AS (
  SELECT 
    cb.customer_id,
    cb.analysis_month,
    COALESCE(ni.network_incident_score, 0) as network_incident_score,
    COALESCE(nst.support_ticket_score, 0) as support_ticket_score,
    COALESCE(up.usage_decline_score, 0) as usage_decline_score,
    COALESCE(sq.service_quality_score, 0) as service_quality_score,
    
    -- Weighted composite score (0-100)
    ROUND(
      (COALESCE(ni.network_incident_score, 0) * 0.40) +
      (COALESCE(nst.support_ticket_score, 0) * 0.30) +
      (COALESCE(up.usage_decline_score, 0) * 0.20) +
      (COALESCE(sq.service_quality_score, 0) * 0.10),
      2
    ) as network_churn_risk_score,
    
    -- Risk level categorization
    CASE 
      WHEN ROUND(
        (COALESCE(ni.network_incident_score, 0) * 0.40) +
        (COALESCE(nst.support_ticket_score, 0) * 0.30) +
        (COALESCE(up.usage_decline_score, 0) * 0.20) +
        (COALESCE(sq.service_quality_score, 0) * 0.10),
        2
      ) >= 70 THEN 'Critical'
      WHEN ROUND(
        (COALESCE(ni.network_incident_score, 0) * 0.40) +
        (COALESCE(nst.support_ticket_score, 0) * 0.30) +
        (COALESCE(up.usage_decline_score, 0) * 0.20) +
        (COALESCE(sq.service_quality_score, 0) * 0.10),
        2
      ) >= 50 THEN 'High'
      WHEN ROUND(
        (COALESCE(ni.network_incident_score, 0) * 0.40) +
        (COALESCE(nst.support_ticket_score, 0) * 0.30) +
        (COALESCE(up.usage_decline_score, 0) * 0.20) +
        (COALESCE(sq.service_quality_score, 0) * 0.10),
        2
      ) >= 30 THEN 'Medium'
      ELSE 'Low'
    END as risk_level,
    
    -- Component counts for analysis
    COALESCE(ni.outage_count, 0) as outage_count,
    COALESCE(ni.unplanned_outages, 0) as unplanned_outages,
    COALESCE(nst.network_ticket_count, 0) as network_ticket_count,
    COALESCE(up.monthly_usage_gb, 0) as current_usage_gb,
    COALESCE(up.prev_month_usage_gb, 0) as prev_month_usage_gb
    
  FROM customer_base cb
  LEFT JOIN network_incidents ni ON cb.customer_id = ni.customer_id AND cb.analysis_month = ni.analysis_month
  LEFT JOIN network_support_tickets nst ON cb.customer_id = nst.customer_id AND cb.analysis_month = nst.analysis_month
  LEFT JOIN usage_patterns up ON cb.customer_id = up.customer_id AND cb.analysis_month = up.analysis_month
  LEFT JOIN service_quality sq ON cb.customer_id = sq.customer_id AND cb.analysis_month = sq.analysis_month
)

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
  prev_month_usage_gb,
  CURRENT_TIMESTAMP() as calculated_at
FROM final_scores
WHERE analysis_month = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()))
ORDER BY network_churn_risk_score DESC