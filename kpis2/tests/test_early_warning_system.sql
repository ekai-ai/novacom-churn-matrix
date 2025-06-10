--
-- Test: Early Warning System Validation
-- Validates business logic and alert prioritization for the early warning system
--

-- Test 1: Ensure only at-risk customers are included (Medium, High, Critical)
SELECT 
  'Low risk customers included in alerts' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('early_warning_system') }}
WHERE risk_level = 'Low'

UNION ALL

-- Test 2: Validate alert severity escalation logic
SELECT 
  'Critical risk without appropriate alert severity' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('early_warning_system') }}
WHERE risk_level = 'Critical' 
  AND alert_severity NOT IN ('High Priority', 'Immediate Action Required')

UNION ALL

-- Test 3: Ensure contact priority score correlates with risk level
SELECT 
  'Contact priority score inconsistent with risk level' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('early_warning_system') }}
WHERE (risk_level = 'Critical' AND contact_priority_score < 70)
   OR (risk_level = 'High' AND contact_priority_score < 50)
   OR (risk_level = 'Medium' AND contact_priority_score < 30)

UNION ALL

-- Test 4: Validate intervention recommendations are not null
SELECT 
  'Missing intervention recommendations' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('early_warning_system') }}
WHERE primary_intervention IS NULL 
   OR secondary_intervention IS NULL

UNION ALL

-- Test 5: Ensure revenue at risk is reasonable (positive and not excessive)
SELECT 
  'Unreasonable revenue at risk values' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('early_warning_system') }}
WHERE potential_revenue_at_risk < 0 
   OR potential_revenue_at_risk > 100000

UNION ALL

-- Test 6: Validate contact timeframe urgency matches risk level
SELECT 
  'Contact timeframe inconsistent with risk level' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('early_warning_system') }}
WHERE (risk_level = 'Critical' AND recommended_contact_timeframe NOT IN ('Within 24 hours', 'Within 3 days'))
   OR (risk_level = 'High' AND recommended_contact_timeframe = 'Within 2 weeks')

UNION ALL

-- Test 7: Ensure target contact date is in the future
SELECT 
  'Target contact date is in the past' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('early_warning_system') }}
WHERE target_contact_date < CURRENT_DATE()

UNION ALL

-- Test 8: Check that high-value customers get appropriate prioritization
SELECT 
  'High-value customers not prioritized appropriately' as test_name,
  COUNT(*) as failed_records
FROM {{ ref('early_warning_system') }}
WHERE customer_tier IN ('Gold', 'Platinum')
  AND monthly_revenue > 500
  AND contact_priority_score < 60