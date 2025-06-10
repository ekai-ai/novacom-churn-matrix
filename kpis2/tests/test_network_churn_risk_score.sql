--
-- Test: Network Churn Risk Score Validation
-- Validates business logic and data quality for the network churn risk score KPI
--

-- Test 1: Ensure all risk scores are within valid range (0-100)
SELECT 
  'Risk score out of range' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('network_churn_risk_score') }}
WHERE network_churn_risk_score < 0 OR network_churn_risk_score > 100

UNION ALL

-- Test 2: Validate risk level categorization logic
SELECT 
  'Risk level categorization incorrect' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('network_churn_risk_score') }}
WHERE (network_churn_risk_score >= 70 AND risk_level != 'Critical')
   OR (network_churn_risk_score >= 50 AND network_churn_risk_score < 70 AND risk_level != 'High')
   OR (network_churn_risk_score >= 30 AND network_churn_risk_score < 50 AND risk_level != 'Medium')
   OR (network_churn_risk_score < 30 AND risk_level != 'Low')

UNION ALL

-- Test 3: Ensure component scores are not greater than composite score
SELECT 
  'Component score exceeds composite score' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('network_churn_risk_score') }}
WHERE network_incident_score > network_churn_risk_score
   OR support_ticket_score > network_churn_risk_score
   OR usage_decline_score > network_churn_risk_score
   OR service_quality_score > network_churn_risk_score

UNION ALL

-- Test 4: Validate that unplanned outages cannot exceed total outages
SELECT 
  'Unplanned outages exceed total outages' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('network_churn_risk_score') }}
WHERE unplanned_outages > outage_count

UNION ALL

-- Test 5: Ensure current month data only
SELECT 
  'Analysis month is not current month' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('network_churn_risk_score') }}
WHERE analysis_month != DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()))

UNION ALL

-- Test 6: Check for reasonable usage values (usage decline score should correlate with usage change)
SELECT 
  'High usage decline score but no usage change' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('network_churn_risk_score') }}
WHERE usage_decline_score > 50 
  AND prev_month_usage_gb > 0 
  AND ABS(prev_month_usage_gb - current_usage_gb) / NULLIF(prev_month_usage_gb, 0) < 0.1