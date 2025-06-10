/*
  Data Quality and Validation Macros for Silver Layer
  
  These macros provide reusable data quality checks and validations
  specifically designed for the silver layer of the medallion architecture.
*/

{% macro silver_data_quality_check(table_name, column_name, check_type='not_null') %}
  /*
    Performs data quality checks on silver layer tables
    
    Args:
      table_name: Name of the table to check
      column_name: Column to validate
      check_type: Type of check ('not_null', 'unique', 'positive', 'email', 'phone')
      
    Returns:
      SQL for data quality validation
  */
  
  {% if check_type == 'not_null' %}
    SELECT COUNT(*) as null_count
    FROM {{ ref(table_name) }}
    WHERE {{ column_name }} IS NULL
    
  {% elif check_type == 'unique' %}
    SELECT {{ column_name }}, COUNT(*) as duplicate_count
    FROM {{ ref(table_name) }}
    GROUP BY {{ column_name }}
    HAVING COUNT(*) > 1
    
  {% elif check_type == 'positive' %}
    SELECT COUNT(*) as negative_count
    FROM {{ ref(table_name) }}
    WHERE {{ column_name }} <= 0
    
  {% elif check_type == 'email' %}
    SELECT COUNT(*) as invalid_email_count
    FROM {{ ref(table_name) }}
    WHERE {{ column_name }} IS NOT NULL 
      AND NOT REGEXP_LIKE({{ column_name }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
      
  {% elif check_type == 'phone' %}
    SELECT COUNT(*) as invalid_phone_count
    FROM {{ ref(table_name) }}
    WHERE {{ column_name }} IS NOT NULL 
      AND NOT REGEXP_LIKE({{ column_name }}, '^[\+]?[1-9][\d]{0,15}$')
      
  {% else %}
    {{ log("Unknown check_type: " ~ check_type, info=True) }}
    SELECT 0 as unknown_check
  {% endif %}
{% endmacro %}

{% macro validate_silver_table(table_name, validations) %}
  /*
    Comprehensive validation for silver layer tables
    
    Args:
      table_name: Name of the silver table
      validations: List of validation dictionaries
        Example: [
          {'column': 'customer_id', 'checks': ['not_null', 'unique']},
          {'column': 'email', 'checks': ['email']},
          {'column': 'revenue', 'checks': ['positive']}
        ]
  */
  
  {% for validation in validations %}
    {% for check in validation.checks %}
      -- Validation: {{ table_name }}.{{ validation.column }} - {{ check }}
      {{ silver_data_quality_check(table_name, validation.column, check) }}
      {% if not loop.last or not loop.last %}UNION ALL{% endif %}
    {% endfor %}
  {% endfor %}
{% endmacro %}

{% macro silver_record_count_check(table_name, min_expected=1) %}
  /*
    Checks if silver table has minimum expected record count
    
    Args:
      table_name: Name of the silver table
      min_expected: Minimum expected record count (default: 1)
  */
  
  SELECT 
    '{{ table_name }}' as table_name,
    COUNT(*) as actual_count,
    {{ min_expected }} as min_expected,
    CASE 
      WHEN COUNT(*) >= {{ min_expected }} THEN 'PASS'
      ELSE 'FAIL'
    END as status
  FROM {{ ref(table_name) }}
{% endmacro %}

{% macro silver_freshness_check(table_name, date_column, max_age_hours=24) %}
  /*
    Checks data freshness in silver layer tables
    
    Args:
      table_name: Name of the silver table
      date_column: Column containing date/timestamp
      max_age_hours: Maximum acceptable age in hours (default: 24)
  */
  
  SELECT 
    '{{ table_name }}' as table_name,
    MAX({{ date_column }}) as latest_record,
    CURRENT_TIMESTAMP() as check_time,
    DATEDIFF('hour', MAX({{ date_column }}), CURRENT_TIMESTAMP()) as age_hours,
    {{ max_age_hours }} as max_age_hours,
    CASE 
      WHEN DATEDIFF('hour', MAX({{ date_column }}), CURRENT_TIMESTAMP()) <= {{ max_age_hours }} THEN 'PASS'
      ELSE 'FAIL'
    END as freshness_status
  FROM {{ ref(table_name) }}
{% endmacro %}