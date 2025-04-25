/*
Business Purpose:
This model creates the customer dimension for analytics, enabling filtering by segment and tier and joining customer metadata to fact tables like tickets and notes.

3NF Construct:
- Selects only atomic customer attributes with no repeating groups or transitive dependencies.
- All non-key columns depend solely on the primary key, customer_id.

Transformations:
- Directly sources from raw.CRM_CUSTOMERS.
- Standardizes casing by lowercasing column names.
- Excludes optional or high‐null columns (e.g., middle_name, address) to reduce sparsity.
*/

{{ config(
    materialized = 'table'
) }}

select
    CUSTOMER_ID   as customer_id,
    FIRST_NAME    as first_name,
    LAST_NAME     as last_name,
    EMAIL         as email,
    CUSTOMER_SEGMENT as customer_segment,
    CUSTOMER_TIER    as customer_tier,
    STATUS           as status
from {{ source('raw', 'CRM_CUSTOMERS') }}