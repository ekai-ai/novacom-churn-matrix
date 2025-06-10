/*
  Silver Layer Transformation Macros
  
  These macros provide reusable transformations for converting bronze data
  to clean, conformed silver layer data following medallion architecture.
*/

{% macro silver_standardize_phone(phone_column) %}
  /*
    Standardizes phone number format for silver layer
    
    Args:
      phone_column: Column containing phone number
      
    Returns:
      Standardized phone number in format +1XXXXXXXXXX
  */
  
  CASE 
    WHEN {{ phone_column }} IS NULL THEN NULL
    WHEN REGEXP_LIKE({{ phone_column }}, '^[\+]?[1-9][\d]{10,15}$') THEN
      REGEXP_REPLACE(
        REGEXP_REPLACE({{ phone_column }}, '[^0-9]', ''),
        '^1?(.{10})$', 
        '+1\\1'
      )
    ELSE NULL
  END
{% endmacro %}

{% macro silver_standardize_email(email_column) %}
  /*
    Standardizes email format for silver layer
    
    Args:
      email_column: Column containing email address
      
    Returns:
      Lowercase, trimmed email or NULL if invalid
  */
  
  CASE 
    WHEN {{ email_column }} IS NULL THEN NULL
    WHEN REGEXP_LIKE(TRIM(LOWER({{ email_column }})), '^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$') THEN
      TRIM(LOWER({{ email_column }}))
    ELSE NULL
  END
{% endmacro %}

{% macro silver_clean_text(text_column, max_length=255) %}
  /*
    Cleans and standardizes text fields for silver layer
    
    Args:
      text_column: Column containing text
      max_length: Maximum allowed length (default: 255)
      
    Returns:
      Cleaned text: trimmed, proper case, limited length
  */
  
  CASE 
    WHEN {{ text_column }} IS NULL THEN NULL
    WHEN TRIM({{ text_column }}) = '' THEN NULL
    ELSE LEFT(
      REGEXP_REPLACE(
        TRIM({{ text_column }}), 
        '\\s+', 
        ' '
      ), 
      {{ max_length }}
    )
  END
{% endmacro %}

{% macro silver_generate_surrogate_key(columns) %}
  /*
    Generates a surrogate key for silver layer dimensions
    
    Args:
      columns: List of column names to include in key generation
      
    Returns:
      MD5 hash of concatenated columns as surrogate key
  */
  
  {% set col_list = [] %}
  {% for col in columns %}
    {% do col_list.append("COALESCE(CAST(" ~ col ~ " AS STRING), 'NULL')") %}
  {% endfor %}
  
  MD5({{ col_list | join(' || \'|\' || ') }})
{% endmacro %}

{% macro silver_scd_type_2_columns() %}
  /*
    Generates standard SCD Type 2 columns for silver dimensions
    
    Returns:
      Standard columns for tracking dimension changes
  */
  
  CURRENT_TIMESTAMP() as EFFECTIVE_DATE,
  '9999-12-31'::DATE as EXPIRATION_DATE,
  TRUE as IS_CURRENT,
  MD5(
    COALESCE(CAST(CUSTOMER_ID AS STRING), 'NULL') || '|' ||
    COALESCE(CAST(CURRENT_TIMESTAMP() AS STRING), 'NULL')
  ) as ROW_HASH,
  CURRENT_TIMESTAMP() as CREATED_AT,
  CURRENT_TIMESTAMP() as UPDATED_AT
{% endmacro %}

{% macro silver_status_standardization(status_column, status_mapping=none) %}
  /*
    Standardizes status values across silver layer tables
    
    Args:
      status_column: Column containing status
      status_mapping: Optional mapping dictionary for custom statuses
      
    Returns:
      Standardized status values
  */
  
  {% if status_mapping %}
    CASE 
      {% for old_status, new_status in status_mapping.items() %}
        WHEN UPPER(TRIM({{ status_column }})) = '{{ old_status.upper() }}' THEN '{{ new_status }}'
      {% endfor %}
      ELSE COALESCE(UPPER(TRIM({{ status_column }})), 'UNKNOWN')
    END
  {% else %}
    CASE 
      WHEN UPPER(TRIM({{ status_column }})) IN ('ACTIVE', 'A', '1', 'TRUE', 'YES', 'Y') THEN 'ACTIVE'
      WHEN UPPER(TRIM({{ status_column }})) IN ('INACTIVE', 'I', '0', 'FALSE', 'NO', 'N') THEN 'INACTIVE'
      WHEN UPPER(TRIM({{ status_column }})) IN ('PENDING', 'P', 'WAIT', 'WAITING') THEN 'PENDING'
      WHEN UPPER(TRIM({{ status_column }})) IN ('CANCELLED', 'CANCELED', 'C', 'CANCEL') THEN 'CANCELLED'
      WHEN UPPER(TRIM({{ status_column }})) IN ('SUSPENDED', 'S', 'SUSPEND') THEN 'SUSPENDED'
      ELSE COALESCE(UPPER(TRIM({{ status_column }})), 'UNKNOWN')
    END
  {% endif %}
{% endmacro %}

{% macro silver_amount_standardization(amount_column, currency_column='USD') %}
  /*
    Standardizes monetary amounts for silver layer
    
    Args:
      amount_column: Column containing monetary amount
      currency_column: Column or literal containing currency code
      
    Returns:
      Standardized amount with validation
  */
  
  CASE 
    WHEN {{ amount_column }} IS NULL THEN NULL
    WHEN {{ amount_column }} < 0 THEN 0  -- Negative amounts set to 0
    ELSE ROUND({{ amount_column }}, 2)
  END
{% endmacro %}

{% macro silver_date_standardization(date_column, format='YYYY-MM-DD') %}
  /*
    Standardizes date formats for silver layer
    
    Args:
      date_column: Column containing date
      format: Expected date format (default: YYYY-MM-DD)
      
    Returns:
      Standardized date or NULL if invalid
  */
  
  CASE 
    WHEN {{ date_column }} IS NULL THEN NULL
    WHEN TRY_CAST({{ date_column }} AS DATE) IS NOT NULL THEN 
      CAST({{ date_column }} AS DATE)
    ELSE NULL
  END
{% endmacro %}