/*
  Testing and Validation Helper Macros
  
  Macros specifically designed for testing silver layer data quality,
  creating test scenarios, and validating business logic.
*/

{% macro silver_test_suite(table_name, test_config) %}
  /*
    Runs a comprehensive test suite for silver layer tables
    
    Args:
      table_name: Name of the silver table to test
      test_config: Dictionary with test configurations
        Example: {
          'required_columns': ['customer_id', 'email'],
          'unique_columns': ['customer_id'],
          'email_columns': ['email'],
          'phone_columns': ['phone'],
          'positive_columns': ['revenue'],
          'min_record_count': 1000,
          'freshness_check': {'column': 'created_at', 'hours': 24}
        }
        
    Returns:
      Comprehensive test results
  */
  
  {% set tests = [] %}
  
  -- Null checks
  {% if test_config.get('required_columns') %}
    {% for col in test_config.required_columns %}
      {% set test_sql %}
        SELECT 
          '{{ table_name }}' as table_name,
          '{{ col }}' as column_name,
          'not_null' as test_type,
          COUNT(*) as fail_count
        FROM {{ ref(table_name) }}
        WHERE {{ col }} IS NULL
      {% endset %}
      {% do tests.append(test_sql) %}
    {% endfor %}
  {% endif %}
  
  -- Uniqueness checks
  {% if test_config.get('unique_columns') %}
    {% for col in test_config.unique_columns %}
      {% set test_sql %}
        SELECT 
          '{{ table_name }}' as table_name,
          '{{ col }}' as column_name,
          'unique' as test_type,
          COUNT(*) - COUNT(DISTINCT {{ col }}) as fail_count
        FROM {{ ref(table_name) }}
      {% endset %}
      {% do tests.append(test_sql) %}
    {% endfor %}
  {% endif %}
  
  -- Email format checks
  {% if test_config.get('email_columns') %}
    {% for col in test_config.email_columns %}
      {% set test_sql %}
        SELECT 
          '{{ table_name }}' as table_name,
          '{{ col }}' as column_name,
          'email_format' as test_type,
          COUNT(*) as fail_count
        FROM {{ ref(table_name) }}
        WHERE {{ col }} IS NOT NULL 
          AND NOT REGEXP_LIKE({{ col }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
      {% endset %}
      {% do tests.append(test_sql) %}
    {% endfor %}
  {% endif %}
  
  -- Record count check
  {% if test_config.get('min_record_count') %}
    {% set test_sql %}
      SELECT 
        '{{ table_name }}' as table_name,
        'record_count' as column_name,
        'min_count' as test_type,
        CASE 
          WHEN COUNT(*) >= {{ test_config.min_record_count }} THEN 0
          ELSE 1
        END as fail_count
      FROM {{ ref(table_name) }}
    {% endset %}
    {% do tests.append(test_sql) %}
  {% endif %}
  
  -- Combine all tests
  {{ tests | join('\nUNION ALL\n') }}
{% endmacro %}

{% macro silver_business_rule_test(table_name, rule_name, rule_sql) %}
  /*
    Tests custom business rules for silver layer data
    
    Args:
      table_name: Name of the table being tested
      rule_name: Descriptive name for the business rule
      rule_sql: SQL expression that should evaluate to FALSE for violations
      
    Returns:
      Business rule test results
  */
  
  SELECT 
    '{{ table_name }}' as table_name,
    '{{ rule_name }}' as rule_name,
    COUNT(*) as violation_count,
    CASE 
      WHEN COUNT(*) = 0 THEN 'PASS'
      ELSE 'FAIL'
    END as test_result
  FROM {{ ref(table_name) }}
  WHERE NOT ({{ rule_sql }})
{% endmacro %}

{% macro silver_referential_integrity_test(child_table, parent_table, child_key, parent_key) %}
  /*
    Tests referential integrity between silver layer tables
    
    Args:
      child_table: Child table name
      parent_table: Parent table name  
      child_key: Foreign key column in child table
      parent_key: Primary key column in parent table
      
    Returns:
      Referential integrity test results
  */
  
  SELECT 
    '{{ child_table }}' as child_table,
    '{{ parent_table }}' as parent_table,
    '{{ child_key }}' as foreign_key_column,
    COUNT(*) as orphaned_records,
    CASE 
      WHEN COUNT(*) = 0 THEN 'PASS'
      ELSE 'FAIL'
    END as test_result
  FROM {{ ref(child_table) }} c
  LEFT JOIN {{ ref(parent_table) }} p ON c.{{ child_key }} = p.{{ parent_key }}
  WHERE c.{{ child_key }} IS NOT NULL 
    AND p.{{ parent_key }} IS NULL
{% endmacro %}

{% macro silver_data_distribution_check(table_name, column_name, expected_distribution) %}
  /*
    Checks if data distribution matches expected patterns
    
    Args:
      table_name: Name of the table
      column_name: Column to check distribution
      expected_distribution: Dict with expected value distributions
        Example: {'Active': 0.8, 'Inactive': 0.2}
        
    Returns:
      Data distribution analysis
  */
  
  WITH distribution_actual AS (
    SELECT 
      {{ column_name }} as value,
      COUNT(*) as actual_count,
      COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () as actual_percentage
    FROM {{ ref(table_name) }}
    GROUP BY {{ column_name }}
  ),
  
  distribution_expected AS (
    {% for value, percentage in expected_distribution.items() %}
    SELECT '{{ value }}' as value, {{ percentage }} as expected_percentage
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
  )
  
  SELECT 
    '{{ table_name }}' as table_name,
    '{{ column_name }}' as column_name,
    a.value,
    a.actual_count,
    a.actual_percentage,
    COALESCE(e.expected_percentage, 0) as expected_percentage,
    ABS(a.actual_percentage - COALESCE(e.expected_percentage, 0)) as percentage_diff,
    CASE 
      WHEN ABS(a.actual_percentage - COALESCE(e.expected_percentage, 0)) <= 0.1 THEN 'PASS'
      ELSE 'FAIL'
    END as test_result
  FROM distribution_actual a
  FULL OUTER JOIN distribution_expected e ON a.value = e.value
{% endmacro %}

{% macro silver_trend_analysis_test(table_name, date_column, metric_column, trend_direction='increasing') %}
  /*
    Tests if metrics are trending in expected direction
    
    Args:
      table_name: Name of the table
      date_column: Date column for trend analysis
      metric_column: Metric to analyze trend
      trend_direction: 'increasing', 'decreasing', or 'stable'
      
    Returns:
      Trend analysis results
  */
  
  WITH monthly_trends AS (
    SELECT 
      DATE_TRUNC('month', {{ date_column }}) as trend_month,
      AVG({{ metric_column }}) as avg_metric
    FROM {{ ref(table_name) }}
    WHERE {{ date_column }} >= DATEADD('month', -6, CURRENT_DATE())
    GROUP BY DATE_TRUNC('month', {{ date_column }})
    ORDER BY trend_month
  ),
  
  trend_calculation AS (
    SELECT 
      COUNT(*) as month_count,
      CORR(EXTRACT(EPOCH FROM trend_month), avg_metric) as correlation,
      CASE 
        {% if trend_direction == 'increasing' %}
        WHEN CORR(EXTRACT(EPOCH FROM trend_month), avg_metric) > 0.5 THEN 'PASS'
        {% elif trend_direction == 'decreasing' %}
        WHEN CORR(EXTRACT(EPOCH FROM trend_month), avg_metric) < -0.5 THEN 'PASS'
        {% elif trend_direction == 'stable' %}
        WHEN ABS(CORR(EXTRACT(EPOCH FROM trend_month), avg_metric)) < 0.3 THEN 'PASS'
        {% endif %}
        ELSE 'FAIL'
      END as test_result
    FROM monthly_trends
  )
  
  SELECT 
    '{{ table_name }}' as table_name,
    '{{ metric_column }}' as metric_column,
    '{{ trend_direction }}' as expected_trend,
    month_count,
    correlation,
    test_result
  FROM trend_calculation
{% endmacro %}

{% macro silver_anomaly_detection(table_name, metric_column, date_column, threshold_std_dev=2) %}
  /*
    Detects anomalies in silver layer metrics
    
    Args:
      table_name: Name of the table
      metric_column: Column to check for anomalies
      date_column: Date column for time-based analysis
      threshold_std_dev: Number of standard deviations to consider anomalous
      
    Returns:
      Anomaly detection results
  */
  
  WITH daily_metrics AS (
    SELECT 
      DATE({{ date_column }}) as metric_date,
      AVG({{ metric_column }}) as daily_avg
    FROM {{ ref(table_name) }}
    WHERE {{ date_column }} >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY DATE({{ date_column }})
  ),
  
  statistical_bounds AS (
    SELECT 
      AVG(daily_avg) as mean_value,
      STDDEV(daily_avg) as std_dev,
      AVG(daily_avg) + ({{ threshold_std_dev }} * STDDEV(daily_avg)) as upper_bound,
      AVG(daily_avg) - ({{ threshold_std_dev }} * STDDEV(daily_avg)) as lower_bound
    FROM daily_metrics
  ),
  
  anomalies AS (
    SELECT 
      dm.metric_date,
      dm.daily_avg,
      sb.mean_value,
      sb.std_dev,
      sb.upper_bound,
      sb.lower_bound,
      CASE 
        WHEN dm.daily_avg > sb.upper_bound OR dm.daily_avg < sb.lower_bound THEN TRUE
        ELSE FALSE
      END as is_anomaly
    FROM daily_metrics dm
    CROSS JOIN statistical_bounds sb
  )
  
  SELECT 
    '{{ table_name }}' as table_name,
    '{{ metric_column }}' as metric_column,
    COUNT(*) as total_days,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_days,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as anomaly_percentage,
    CASE 
      WHEN SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) * 1.0 / COUNT(*) > 0.1 THEN 'INVESTIGATE'
      ELSE 'NORMAL'
    END as alert_status
  FROM anomalies
{% endmacro %}