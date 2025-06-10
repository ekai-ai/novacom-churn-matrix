/*
  Utility and Date Macros for Silver Layer
  
  General-purpose utility macros for common transformations,
  date operations, and helper functions used across silver layer models.
*/

{% macro silver_safe_divide(numerator, denominator, default_value=0) %}
  /*
    Performs safe division avoiding division by zero
    
    Args:
      numerator: Numerator value
      denominator: Denominator value  
      default_value: Value to return when denominator is 0 (default: 0)
      
    Returns:
      Safe division result
  */
  
  CASE 
    WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN {{ default_value }}
    ELSE {{ numerator }} / {{ denominator }}
  END
{% endmacro %}

{% macro silver_coalesce_chain(columns, default_value='Unknown') %}
  /*
    Creates a coalesce chain across multiple columns
    
    Args:
      columns: List of column names to coalesce
      default_value: Final default value (default: 'Unknown')
      
    Returns:
      Coalesced value
  */
  
  COALESCE(
    {% for col in columns %}
    NULLIF(TRIM({{ col }}), '')
    {%- if not loop.last %},{% endif %}
    {% endfor %}
    , '{{ default_value }}'
  )
{% endmacro %}

{% macro silver_age_calculation(birth_date, as_of_date='CURRENT_DATE()') %}
  /*
    Calculates age from birth date
    
    Args:
      birth_date: Birth date column
      as_of_date: Date to calculate age as of (default: current date)
      
    Returns:
      Age in years
  */
  
  CASE 
    WHEN {{ birth_date }} IS NULL THEN NULL
    WHEN {{ birth_date }} > {{ as_of_date }} THEN NULL  -- Future birth date
    ELSE DATEDIFF('year', {{ birth_date }}, {{ as_of_date }})
  END
{% endmacro %}

{% macro silver_fiscal_year(date_column, fiscal_year_start_month=1) %}
  /*
    Calculates fiscal year from date
    
    Args:
      date_column: Date column
      fiscal_year_start_month: Month when fiscal year starts (default: 1 for January)
      
    Returns:
      Fiscal year
  */
  
  CASE 
    WHEN {{ date_column }} IS NULL THEN NULL
    WHEN MONTH({{ date_column }}) >= {{ fiscal_year_start_month }} THEN YEAR({{ date_column }})
    ELSE YEAR({{ date_column }}) - 1
  END
{% endmacro %}

{% macro silver_quarter_name(date_column, format='Q1 2023') %}
  /*
    Generates quarter name from date
    
    Args:
      date_column: Date column
      format: Format template (default: 'Q1 2023')
      
    Returns:
      Formatted quarter name
  */
  
  CASE 
    WHEN {{ date_column }} IS NULL THEN 'Unknown Quarter'
    ELSE 'Q' || QUARTER({{ date_column }}) || ' ' || YEAR({{ date_column }})
  END
{% endmacro %}

{% macro silver_month_name(date_column, format='full') %}
  /*
    Generates month name from date
    
    Args:
      date_column: Date column  
      format: 'full', 'short', or 'number' (default: 'full')
      
    Returns:
      Formatted month name
  */
  
  {% if format == 'full' %}
    CASE 
      WHEN {{ date_column }} IS NULL THEN 'Unknown Month'
      ELSE MONTHNAME({{ date_column }})
    END
  {% elif format == 'short' %}
    CASE 
      WHEN {{ date_column }} IS NULL THEN 'UNK'
      ELSE LEFT(MONTHNAME({{ date_column }}), 3)
    END
  {% elif format == 'number' %}
    CASE 
      WHEN {{ date_column }} IS NULL THEN 0
      ELSE MONTH({{ date_column }})
    END
  {% endif %}
{% endmacro %}

{% macro silver_business_days_between(start_date, end_date) %}
  /*
    Calculates business days between two dates (excludes weekends)
    
    Args:
      start_date: Start date
      end_date: End date
      
    Returns:
      Number of business days
  */
  
  CASE 
    WHEN {{ start_date }} IS NULL OR {{ end_date }} IS NULL THEN NULL
    WHEN {{ end_date }} < {{ start_date }} THEN 0
    ELSE 
      DATEDIFF('day', {{ start_date }}, {{ end_date }}) - 
      (DATEDIFF('week', {{ start_date }}, {{ end_date }}) * 2) -
      CASE WHEN DAYOFWEEK({{ start_date }}) = 1 THEN 1 ELSE 0 END -  -- Sunday start
      CASE WHEN DAYOFWEEK({{ end_date }}) = 7 THEN 1 ELSE 0 END      -- Saturday end
  END
{% endmacro %}

{% macro silver_tenure_calculation(start_date, end_date='CURRENT_DATE()', unit='months') %}
  /*
    Calculates tenure/duration between dates
    
    Args:
      start_date: Start date (e.g., hire date, customer start)
      end_date: End date (default: current date)
      unit: 'days', 'months', or 'years' (default: 'months')
      
    Returns:
      Tenure in specified unit
  */
  
  CASE 
    WHEN {{ start_date }} IS NULL THEN NULL
    WHEN {{ start_date }} > {{ end_date }} THEN 0
    {% if unit == 'days' %}
    ELSE DATEDIFF('day', {{ start_date }}, {{ end_date }})
    {% elif unit == 'months' %}  
    ELSE DATEDIFF('month', {{ start_date }}, {{ end_date }})
    {% elif unit == 'years' %}
    ELSE DATEDIFF('year', {{ start_date }}, {{ end_date }})
    {% endif %}
  END
{% endmacro %}

{% macro silver_categorize_amount(amount_column, low_threshold=100, high_threshold=1000) %}
  /*
    Categorizes amounts into Low/Medium/High buckets
    
    Args:
      amount_column: Amount column to categorize
      low_threshold: Threshold for low amounts (default: 100)
      high_threshold: Threshold for high amounts (default: 1000)
      
    Returns:
      Amount category
  */
  
  CASE 
    WHEN {{ amount_column }} IS NULL THEN 'Unknown'
    WHEN {{ amount_column }} <= 0 THEN 'Zero or Negative'
    WHEN {{ amount_column }} <= {{ low_threshold }} THEN 'Low'
    WHEN {{ amount_column }} <= {{ high_threshold }} THEN 'Medium'
    ELSE 'High'
  END
{% endmacro %}

{% macro silver_hash_pii(column_name, salt='silver_layer_salt') %}
  /*
    Hashes PII data for silver layer privacy compliance
    
    Args:
      column_name: Column containing PII
      salt: Salt value for hashing (default: 'silver_layer_salt')
      
    Returns:
      Hashed value maintaining referential integrity
  */
  
  CASE 
    WHEN {{ column_name }} IS NULL THEN NULL
    ELSE SHA2(CONCAT('{{ salt }}', UPPER(TRIM({{ column_name }}))), 256)
  END
{% endmacro %}

{% macro silver_current_timestamp_utc() %}
  /*
    Gets current timestamp in UTC for silver layer consistency
    
    Returns:
      Current timestamp in UTC
  */
  
  CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())
{% endmacro %}