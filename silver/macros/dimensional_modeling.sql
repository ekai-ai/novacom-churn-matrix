/*
  Dimensional Modeling Helper Macros
  
  These macros implement Kimball methodology patterns for building
  silver layer dimensions and facts following best practices.
*/

{% macro silver_dim_config(unique_key, sort_keys=[], dist_key=none) %}
  /*
    Standard configuration for silver layer dimension tables
    
    Args:
      unique_key: Primary key column(s)
      sort_keys: List of columns to sort by
      dist_key: Distribution key for optimization
      
    Returns:
      Standard dimension table configuration
  */
  
  {{
    config(
      materialized='table',
      unique_key=unique_key,
      sort=sort_keys,
      dist=dist_key if dist_key else unique_key,
      schema='silver',
      tags=['silver', 'dimension'],
      pre_hook="SET query_group = 'silver_dimensions'",
      post_hook="GRANT SELECT ON TABLE {{ this }} TO GROUP silver_readers"
    )
  }}
{% endmacro %}

{% macro silver_fact_config(partition_by=none, cluster_by=[], dist_key=none) %}
  /*
    Standard configuration for silver layer fact tables
    
    Args:
      partition_by: Column to partition by (usually date)
      cluster_by: List of columns to cluster by
      dist_key: Distribution key for optimization
      
    Returns:
      Standard fact table configuration
  */
  
  {{
    config(
      materialized='table',
      partition_by=partition_by,
      cluster_by=cluster_by,
      dist=dist_key,
      schema='silver',
      tags=['silver', 'fact'],
      pre_hook="SET query_group = 'silver_facts'",
      post_hook="GRANT SELECT ON TABLE {{ this }} TO GROUP silver_readers"
    )
  }}
{% endmacro %}

{% macro generate_dim_customer_key(customer_id) %}
  /*
    Generates standardized customer dimension key
    
    Args:
      customer_id: Natural customer identifier
      
    Returns:
      Standardized customer surrogate key
  */
  
  CASE 
    WHEN {{ customer_id }} IS NULL THEN '-1'  -- Unknown customer
    ELSE 'CUST_' || LPAD(CAST({{ customer_id }} AS STRING), 10, '0')
  END
{% endmacro %}

{% macro generate_date_key(date_column) %}
  /*
    Generates standardized date key for fact table relationships
    
    Args:
      date_column: Date column to convert
      
    Returns:
      Integer date key in YYYYMMDD format
  */
  
  CASE 
    WHEN {{ date_column }} IS NULL THEN -1  -- Unknown date
    ELSE CAST(
      YEAR({{ date_column }}) * 10000 + 
      MONTH({{ date_column }}) * 100 + 
      DAY({{ date_column }}) AS INT
    )
  END
{% endmacro %}

{% macro silver_unknown_member(dimension_name, key_column, key_value='-1') %}
  /*
    Creates unknown member record for dimension tables
    
    Args:
      dimension_name: Name of the dimension
      key_column: Primary key column name
      key_value: Value for unknown key (default: '-1')
      
    Returns:
      Unknown member record SQL
  */
  
  SELECT 
    '{{ key_value }}' as {{ key_column }},
    'Unknown' as {{ dimension_name.replace('_', ' ').title().replace(' ', '') }}_NAME,
    'Not Available' as DESCRIPTION,
    'UNKNOWN' as STATUS,
    '1900-01-01'::DATE as EFFECTIVE_DATE,
    '9999-12-31'::DATE as EXPIRATION_DATE,
    TRUE as IS_CURRENT,
    'SYSTEM' as CREATED_BY,
    '1900-01-01 00:00:00'::TIMESTAMP as CREATED_AT,
    '1900-01-01 00:00:00'::TIMESTAMP as UPDATED_AT
{% endmacro %}

{% macro silver_fact_date_spine(start_date, end_date, grain='day') %}
  /*
    Generates date spine for fact table completeness
    
    Args:
      start_date: Start date for spine
      end_date: End date for spine  
      grain: Date granularity ('day', 'hour', 'month')
      
    Returns:
      Date spine with appropriate grain
  */
  
  {% if grain == 'day' %}
    WITH date_spine AS (
      SELECT 
        DATE_ADD('day', ROW_NUMBER() OVER (ORDER BY 1) - 1, '{{ start_date }}'::DATE) as spine_date
      FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF('day', '{{ start_date }}'::DATE, '{{ end_date }}'::DATE) + 1))
    )
    SELECT 
      spine_date,
      {{ generate_date_key('spine_date') }} as date_key
    FROM date_spine
    
  {% elif grain == 'hour' %}
    WITH hour_spine AS (
      SELECT 
        DATEADD('hour', ROW_NUMBER() OVER (ORDER BY 1) - 1, '{{ start_date }}'::TIMESTAMP) as spine_hour
      FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF('hour', '{{ start_date }}'::TIMESTAMP, '{{ end_date }}'::TIMESTAMP) + 1))
    )
    SELECT 
      spine_hour,
      {{ generate_date_key('spine_hour::DATE') }} as date_key,
      HOUR(spine_hour) as hour_key
    FROM hour_spine
    
  {% elif grain == 'month' %}
    WITH month_spine AS (
      SELECT 
        DATE_TRUNC('month', DATEADD('month', ROW_NUMBER() OVER (ORDER BY 1) - 1, '{{ start_date }}'::DATE)) as spine_month
      FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF('month', '{{ start_date }}'::DATE, '{{ end_date }}'::DATE) + 1))
    )
    SELECT 
      spine_month,
      {{ generate_date_key('spine_month') }} as date_key
    FROM month_spine
  {% endif %}
{% endmacro %}

{% macro silver_slowly_changing_dimension(source_table, natural_key, tracked_columns, meta_columns=[]) %}
  /*
    Implements SCD Type 2 logic for silver layer dimensions
    
    Args:
      source_table: Source staging table
      natural_key: Business key column(s)
      tracked_columns: Columns to track for changes
      meta_columns: Additional metadata columns to include
      
    Returns:
      SCD Type 2 dimension logic
  */
  
  WITH source_data AS (
    SELECT 
      {{ natural_key }},
      {% for col in tracked_columns %}
      {{ col }},
      {% endfor %}
      {% for col in meta_columns %}
      {{ col }},
      {% endfor %}
      MD5(CONCAT(
        {% for col in tracked_columns %}
        COALESCE(CAST({{ col }} AS STRING), 'NULL')
        {%- if not loop.last %} || '|' || {% endif %}
        {% endfor %}
      )) as ROW_HASH,
      CURRENT_TIMESTAMP() as EFFECTIVE_DATE
    FROM {{ ref(source_table) }}
  ),
  
  existing_data AS (
    SELECT *
    FROM {{ this }}
    WHERE IS_CURRENT = TRUE
  ),
  
  changed_records AS (
    SELECT 
      s.*,
      CASE 
        WHEN e.{{ natural_key }} IS NULL THEN 'INSERT'
        WHEN s.ROW_HASH != e.ROW_HASH THEN 'UPDATE'
        ELSE 'NO_CHANGE'
      END as CHANGE_TYPE
    FROM source_data s
    LEFT JOIN existing_data e ON s.{{ natural_key }} = e.{{ natural_key }}
  )
  
  -- Insert new records and updates
  SELECT 
    {{ natural_key }},
    {% for col in tracked_columns %}
    {{ col }},
    {% endfor %}
    {% for col in meta_columns %}
    {{ col }},
    {% endfor %}
    ROW_HASH,
    EFFECTIVE_DATE,
    '9999-12-31'::DATE as EXPIRATION_DATE,
    TRUE as IS_CURRENT,
    CURRENT_TIMESTAMP() as CREATED_AT,
    CURRENT_TIMESTAMP() as UPDATED_AT
  FROM changed_records
  WHERE CHANGE_TYPE IN ('INSERT', 'UPDATE')
  
  UNION ALL
  
  -- Expire old records for updates
  SELECT 
    e.{{ natural_key }},
    {% for col in tracked_columns %}
    e.{{ col }},
    {% endfor %}
    {% for col in meta_columns %}
    e.{{ col }},
    {% endfor %}
    e.ROW_HASH,
    e.EFFECTIVE_DATE,
    CURRENT_DATE() as EXPIRATION_DATE,
    FALSE as IS_CURRENT,
    e.CREATED_AT,
    CURRENT_TIMESTAMP() as UPDATED_AT
  FROM existing_data e
  INNER JOIN changed_records c ON e.{{ natural_key }} = c.{{ natural_key }}
  WHERE c.CHANGE_TYPE = 'UPDATE'
{% endmacro %}