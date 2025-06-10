/*
  Telecom-Specific Macros for Silver Layer
  
  Industry-specific macros for telecom data transformations,
  business logic, and domain-specific calculations.
*/

{% macro silver_customer_segment_logic(monthly_revenue, service_count, account_type='Individual') %}
  /*
    Determines customer segment based on telecom business rules
    
    Args:
      monthly_revenue: Monthly revenue amount
      service_count: Number of active services  
      account_type: Type of account (default: 'Individual')
      
    Returns:
      Customer segment classification
  */
  
  CASE 
    WHEN {{ account_type }} = 'Government' THEN 'Government'
    WHEN {{ account_type }} = 'Business' AND {{ monthly_revenue }} >= 1000 THEN 'Enterprise' 
    WHEN {{ account_type }} = 'Business' AND {{ monthly_revenue }} >= 200 THEN 'SMB'
    WHEN {{ account_type }} = 'Business' THEN 'Small Business'
    WHEN {{ monthly_revenue }} >= 150 OR {{ service_count }} >= 3 THEN 'Premium Residential'
    WHEN {{ monthly_revenue }} >= 75 THEN 'Standard Residential'
    WHEN {{ monthly_revenue }} > 0 THEN 'Basic Residential'
    ELSE 'Inactive'
  END
{% endmacro %}

{% macro silver_customer_tier_logic(tenure_months, monthly_revenue, payment_history_score=100) %}
  /*
    Determines customer tier based on value and loyalty
    
    Args:
      tenure_months: Customer tenure in months
      monthly_revenue: Average monthly revenue
      payment_history_score: Payment reliability score (default: 100)
      
    Returns:
      Customer tier (Bronze, Silver, Gold, Platinum)
  */
  
  CASE 
    WHEN {{ monthly_revenue }} >= 200 AND {{ tenure_months }} >= 24 AND {{ payment_history_score }} >= 90 THEN 'Platinum'
    WHEN {{ monthly_revenue }} >= 100 AND {{ tenure_months }} >= 12 AND {{ payment_history_score }} >= 80 THEN 'Gold'
    WHEN {{ monthly_revenue }} >= 50 AND {{ tenure_months }} >= 6 AND {{ payment_history_score }} >= 70 THEN 'Silver'
    WHEN {{ monthly_revenue }} > 0 THEN 'Bronze'
    ELSE 'Inactive'
  END
{% endmacro %}

{% macro silver_service_type_standardization(service_name) %}
  /*
    Standardizes telecom service types
    
    Args:
      service_name: Raw service name
      
    Returns:
      Standardized service type
  */
  
  CASE 
    WHEN UPPER({{ service_name }}) LIKE '%INTERNET%' OR UPPER({{ service_name }}) LIKE '%BROADBAND%' THEN 'Internet'
    WHEN UPPER({{ service_name }}) LIKE '%MOBILE%' OR UPPER({{ service_name }}) LIKE '%CELL%' OR UPPER({{ service_name }}) LIKE '%WIRELESS%' THEN 'Mobile'
    WHEN UPPER({{ service_name }}) LIKE '%TV%' OR UPPER({{ service_name }}) LIKE '%TELEVISION%' OR UPPER({{ service_name }}) LIKE '%CABLE%' THEN 'TV'
    WHEN UPPER({{ service_name }}) LIKE '%PHONE%' OR UPPER({{ service_name }}) LIKE '%LANDLINE%' OR UPPER({{ service_name }}) LIKE '%VOICE%' THEN 'Voice'
    WHEN UPPER({{ service_name }}) LIKE '%BUNDLE%' OR UPPER({{ service_name }}) LIKE '%PACKAGE%' THEN 'Bundle'
    WHEN UPPER({{ service_name }}) LIKE '%BUSINESS%' THEN 'Business Services'
    WHEN UPPER({{ service_name }}) LIKE '%CLOUD%' OR UPPER({{ service_name }}) LIKE '%DATA%' THEN 'Cloud & Data'
    ELSE 'Other'
  END
{% endmacro %}

{% macro silver_churn_risk_indicators(days_since_last_payment, support_tickets_count, usage_trend, contract_end_days) %}
  /*
    Calculates churn risk indicators for telecom customers
    
    Args:
      days_since_last_payment: Days since last payment
      support_tickets_count: Number of support tickets (last 90 days)
      usage_trend: Usage trend percentage (negative = declining)
      contract_end_days: Days until contract end
      
    Returns:
      Churn risk score (0-100)
  */
  
  LEAST(100, GREATEST(0,
    -- Payment risk (0-30 points)
    CASE 
      WHEN {{ days_since_last_payment }} > 60 THEN 30
      WHEN {{ days_since_last_payment }} > 30 THEN 20  
      WHEN {{ days_since_last_payment }} > 15 THEN 10
      ELSE 0
    END +
    
    -- Support ticket risk (0-25 points)
    CASE 
      WHEN {{ support_tickets_count }} >= 5 THEN 25
      WHEN {{ support_tickets_count }} >= 3 THEN 15
      WHEN {{ support_tickets_count }} >= 1 THEN 5
      ELSE 0
    END +
    
    -- Usage trend risk (0-25 points)
    CASE 
      WHEN {{ usage_trend }} <= -50 THEN 25
      WHEN {{ usage_trend }} <= -25 THEN 15
      WHEN {{ usage_trend }} <= -10 THEN 8
      ELSE 0
    END +
    
    -- Contract end risk (0-20 points)
    CASE 
      WHEN {{ contract_end_days }} <= 30 THEN 20
      WHEN {{ contract_end_days }} <= 60 THEN 10
      WHEN {{ contract_end_days }} <= 90 THEN 5
      ELSE 0
    END
  ))
{% endmacro %}

{% macro silver_network_quality_score(outage_minutes, avg_speed_mbps, target_speed_mbps, uptime_percentage) %}
  /*
    Calculates network quality score for telecom services
    
    Args:
      outage_minutes: Total outage minutes in period
      avg_speed_mbps: Average speed delivered
      target_speed_mbps: Promised speed 
      uptime_percentage: Uptime percentage
      
    Returns:
      Network quality score (0-100)
  */
  
  LEAST(100, GREATEST(0,
    -- Base score
    100 -
    
    -- Outage penalty (up to 40 points)
    LEAST(40, {{ outage_minutes }} / 10.0) -
    
    -- Speed penalty (up to 30 points)  
    CASE 
      WHEN {{ avg_speed_mbps }} / NULLIF({{ target_speed_mbps }}, 0) >= 0.9 THEN 0
      WHEN {{ avg_speed_mbps }} / NULLIF({{ target_speed_mbps }}, 0) >= 0.7 THEN 10
      WHEN {{ avg_speed_mbps }} / NULLIF({{ target_speed_mbps }}, 0) >= 0.5 THEN 20
      ELSE 30
    END -
    
    -- Uptime penalty (up to 30 points)
    CASE 
      WHEN {{ uptime_percentage }} >= 99.9 THEN 0
      WHEN {{ uptime_percentage }} >= 99.5 THEN 10
      WHEN {{ uptime_percentage }} >= 99.0 THEN 20
      ELSE 30
    END
  ))
{% endmacro %}

{% macro silver_usage_category(data_gb, voice_minutes, service_type) %}
  /*
    Categorizes telecom usage patterns
    
    Args:
      data_gb: Data usage in GB
      voice_minutes: Voice usage in minutes
      service_type: Type of service
      
    Returns:
      Usage category
  */
  
  CASE 
    WHEN '{{ service_type }}' = 'Internet' THEN
      CASE 
        WHEN {{ data_gb }} >= 500 THEN 'Heavy Data User'
        WHEN {{ data_gb }} >= 100 THEN 'Moderate Data User'
        WHEN {{ data_gb }} >= 20 THEN 'Light Data User'
        WHEN {{ data_gb }} > 0 THEN 'Minimal Data User'
        ELSE 'No Usage'
      END
    WHEN '{{ service_type }}' = 'Mobile' THEN
      CASE 
        WHEN {{ data_gb }} >= 50 AND {{ voice_minutes }} >= 1000 THEN 'Heavy Mobile User'
        WHEN {{ data_gb }} >= 20 OR {{ voice_minutes }} >= 500 THEN 'Moderate Mobile User'
        WHEN {{ data_gb }} >= 5 OR {{ voice_minutes }} >= 100 THEN 'Light Mobile User'
        WHEN {{ data_gb }} > 0 OR {{ voice_minutes }} > 0 THEN 'Minimal Mobile User'
        ELSE 'No Usage'
      END
    WHEN '{{ service_type }}' = 'Voice' THEN
      CASE 
        WHEN {{ voice_minutes }} >= 2000 THEN 'Heavy Voice User'
        WHEN {{ voice_minutes }} >= 500 THEN 'Moderate Voice User'
        WHEN {{ voice_minutes }} >= 100 THEN 'Light Voice User'
        WHEN {{ voice_minutes }} > 0 THEN 'Minimal Voice User'
        ELSE 'No Usage'
      END
    ELSE 'Unknown Service'
  END
{% endmacro %}

{% macro silver_contract_status(start_date, end_date, current_date='CURRENT_DATE()') %}
  /*
    Determines telecom contract status
    
    Args:
      start_date: Contract start date
      end_date: Contract end date
      current_date: Current date for evaluation
      
    Returns:
      Contract status
  */
  
  CASE 
    WHEN {{ start_date }} IS NULL OR {{ end_date }} IS NULL THEN 'Unknown'
    WHEN {{ current_date }} < {{ start_date }} THEN 'Future'
    WHEN {{ current_date }} > {{ end_date }} THEN 'Expired'
    WHEN DATEDIFF('day', {{ current_date }}, {{ end_date }}) <= 30 THEN 'Expiring Soon'
    WHEN DATEDIFF('day', {{ current_date }}, {{ end_date }}) <= 90 THEN 'Expiring'
    ELSE 'Active'
  END
{% endmacro %}