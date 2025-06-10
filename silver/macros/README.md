# Silver Layer Macros Documentation

This directory contains reusable dbt macros specifically designed for the silver layer of the medallion architecture. These macros implement best practices for data quality, transformations, and dimensional modeling in the telecom domain.

## 📁 Macro Categories

### 1. **data_quality.sql** - Data Quality & Validation
Macros for ensuring data quality and validating silver layer transformations.

#### Key Macros:
- `silver_data_quality_check()` - Performs various data quality checks
- `validate_silver_table()` - Comprehensive table validation
- `silver_record_count_check()` - Validates minimum record counts
- `silver_freshness_check()` - Checks data freshness

#### Usage Examples:
```sql
-- Check for null values in customer_id
{{ silver_data_quality_check('silver_dim_customer', 'customer_id', 'not_null') }}

-- Validate email format
{{ silver_data_quality_check('silver_dim_customer', 'email', 'email') }}

-- Check data freshness (last 24 hours)
{{ silver_freshness_check('silver_fact_billing', 'invoice_date', 24) }}
```

### 2. **silver_transformations.sql** - Data Transformations
Macros for standardizing and cleaning data during bronze to silver transformation.

#### Key Macros:
- `silver_standardize_phone()` - Standardizes phone numbers
- `silver_standardize_email()` - Cleans and validates emails
- `silver_clean_text()` - Standardizes text fields
- `silver_generate_surrogate_key()` - Creates surrogate keys
- `silver_status_standardization()` - Standardizes status values

#### Usage Examples:
```sql
-- Standardize phone number
{{ silver_standardize_phone('raw_phone') }} as phone_standardized

-- Clean email addresses
{{ silver_standardize_email('raw_email') }} as email_clean

-- Generate surrogate key
{{ silver_generate_surrogate_key(['customer_id', 'account_id']) }} as customer_sk

-- Standardize status values
{{ silver_status_standardization('raw_status') }} as status_clean
```

### 3. **dimensional_modeling.sql** - Kimball Methodology
Macros implementing Kimball dimensional modeling patterns for silver layer.

#### Key Macros:
- `silver_dim_config()` - Standard dimension table configuration
- `silver_fact_config()` - Standard fact table configuration
- `generate_dim_customer_key()` - Customer dimension key generation
- `generate_date_key()` - Date key for fact tables
- `silver_slowly_changing_dimension()` - SCD Type 2 implementation

#### Usage Examples:
```sql
-- Dimension table configuration
{{ silver_dim_config('customer_sk', ['customer_id', 'start_date'], 'customer_id') }}

-- Generate date key
{{ generate_date_key('invoice_date') }} as invoice_date_key

-- Customer key generation
{{ generate_dim_customer_key('customer_id') }} as customer_sk
```

### 4. **utilities.sql** - General Utilities
General-purpose utility macros for common calculations and transformations.

#### Key Macros:
- `silver_safe_divide()` - Division with zero handling
- `silver_coalesce_chain()` - Multi-column coalesce
- `silver_age_calculation()` - Age calculation from birth date
- `silver_tenure_calculation()` - Tenure/duration calculations
- `silver_categorize_amount()` - Amount categorization

#### Usage Examples:
```sql
-- Safe division for ARPU calculation
{{ silver_safe_divide('total_revenue', 'customer_count', 0) }} as arpu

-- Calculate customer age
{{ silver_age_calculation('birth_date') }} as customer_age

-- Calculate tenure in months
{{ silver_tenure_calculation('start_date', 'CURRENT_DATE()', 'months') }} as tenure_months

-- Categorize revenue amounts
{{ silver_categorize_amount('monthly_revenue', 100, 500) }} as revenue_category
```

### 5. **telecom_specific.sql** - Industry-Specific Logic
Telecom industry-specific business logic and calculations.

#### Key Macros:
- `silver_customer_segment_logic()` - Customer segmentation rules
- `silver_customer_tier_logic()` - Customer tier assignment
- `silver_service_type_standardization()` - Service type mapping
- `silver_churn_risk_indicators()` - Churn risk scoring
- `silver_network_quality_score()` - Network quality metrics

#### Usage Examples:
```sql
-- Determine customer segment
{{ silver_customer_segment_logic('monthly_revenue', 'service_count', 'account_type') }} as customer_segment

-- Calculate churn risk score
{{ silver_churn_risk_indicators('days_since_payment', 'ticket_count', 'usage_trend', 'contract_days') }} as churn_risk_score

-- Network quality scoring
{{ silver_network_quality_score('outage_minutes', 'avg_speed', 'target_speed', 'uptime_pct') }} as quality_score

-- Standardize service types
{{ silver_service_type_standardization('service_name') }} as service_type_standard
```

## 🎯 Best Practices

### Configuration Macros
Always use the configuration macros for consistent table setup:

```sql
-- For dimension tables
{{ silver_dim_config('customer_sk', ['customer_id'], 'customer_id') }}

-- For fact tables  
{{ silver_fact_config('invoice_date', ['customer_id', 'invoice_date'], 'customer_id') }}
```

### Data Quality Integration
Incorporate data quality checks in your models:

```sql
-- In your silver dimension model
SELECT 
  {{ silver_generate_surrogate_key(['customer_id']) }} as customer_sk,
  customer_id,
  {{ silver_standardize_email('email') }} as email,
  {{ silver_standardize_phone('phone') }} as phone,
  {{ silver_clean_text('first_name') }} as first_name,
  {{ silver_customer_segment_logic('monthly_revenue', 'service_count') }} as segment
FROM {{ ref('stg_crm_customers') }}
```

### Chaining Macros
Macros can be chained for complex transformations:

```sql
SELECT 
  customer_id,
  {{ silver_categorize_amount(
    silver_safe_divide('total_revenue', 'active_months', 0), 
    50, 200
  ) }} as arpu_category
FROM customer_summary
```

## 🔍 Testing Your Macros

Create tests to validate macro behavior:

```sql
-- Test data quality macro
SELECT {{ silver_data_quality_check('silver_dim_customer', 'email', 'email') }}

-- Test standardization
SELECT 
  '  Test@Example.COM  ' as original,
  {{ silver_standardize_email("'  Test@Example.COM  '") }} as standardized
```

## 🚀 Performance Tips

1. **Use appropriate distribution keys** in configuration macros
2. **Sort keys** should match common query patterns  
3. **Surrogate key generation** should be consistent across related tables
4. **Data quality checks** should be run as separate test models, not inline

## 📝 Contributing

When adding new macros:

1. **Follow naming convention**: `silver_[category]_[function]()`
2. **Add comprehensive documentation** with args and examples
3. **Include error handling** for edge cases
4. **Test thoroughly** with sample data
5. **Update this README** with usage examples

## 🔗 Integration with Models

These macros are designed to work seamlessly with your silver layer models. Import them by ensuring your `dbt_project.yml` includes the macro paths:

```yaml
macro-paths: ["macros"]
```

Then use them in any model within the silver layer project.