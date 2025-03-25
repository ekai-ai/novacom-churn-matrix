{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_id',
    partition_by={'field': 'join_date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['customer_id'],
    tags=['daily', 'mart', 'features'],
    docs={'show': True},
    comment='Denormalized table providing a 360-degree view of customers for churn prediction modeling. Contains customer profile, service, billing, usage, support, and marketing interaction data.'
) }}

WITH customers AS (
    SELECT
        customer_id,
        anonymized_demographics,
        anonymized_location,
        join_date,
        status -- Needed for Label metric
    FROM {{ ref('stg_customers') }} -- or source('your_source', 'Customers')
    {% if is_incremental() %}
    -- Optional: Filter for incremental loads if applicable based on source updates
    -- WHERE updated_at > (SELECT MAX(dbt_updated_at) FROM {{ this }})
    {% endif %}
),

accounts AS (
    SELECT
        customer_id,
        account_type,
        contract_term,
        preferred_payment_method
    FROM {{ ref('stg_accounts') }} -- or source('your_source', 'Accounts')
),

customer_services AS (
    SELECT
        customer_id,
        service_id,
        status,
        monthly_charge
        -- Add flag or logic if needed to identify primary service
    FROM {{ ref('stg_customer_services') }} -- or source('your_source', 'Customer_Services')
),

services AS (
    SELECT
        service_id,
        service_type
    FROM {{ ref('stg_services') }} -- or source('your_source', 'Services')
),

billing AS (
    SELECT
        customer_id,
        billing_id,
        billing_date,
        amount_due,
        payment_status,
        payment_date
    FROM {{ ref('stg_billing') }} -- or source('your_source', 'Billing')
),

support_tickets AS (
    SELECT
        customer_id,
        ticket_id,
        ticket_creation_date,
        ticket_status,
        ticket_resolution_date
    FROM {{ ref('stg_support_tickets') }} -- or source('your_source', 'Support_Tickets')
),

usage AS (
    SELECT
        customer_id,
        usage_month, -- Assuming monthly granularity
        data_usage_gb,
        call_minutes,
        sms_count
    FROM {{ ref('stg_usage') }} -- or source('your_source', 'Usage')
),

marketing_interactions AS (
    SELECT
        customer_id,
        interaction_id,
        interaction_date,
        response -- Assuming boolean or similar
    FROM {{ ref('stg_marketing_interactions') }} -- or source('your_source', 'Marketing_Interactions')
),

-- Pre-aggregate metrics to ensure correct grain (customer_id)

customer_services_agg AS (
    SELECT
        cs.customer_id,
        COUNT(DISTINCT CASE WHEN cs.status = 'Active' THEN cs.service_id END) AS active_services_count,
        SUM(CASE WHEN cs.status = 'Active' THEN cs.monthly_charge ELSE 0 END) AS total_monthly_charge_active_services,
        AVG(CASE WHEN cs.status = 'Active' THEN cs.monthly_charge END) AS avg_monthly_charge_active_services,
        -- Logic for primary_service_type needs to be defined based on business rules
        -- Example: MAX(CASE WHEN <condition for primary> THEN s.service_type END) AS primary_service_type
        'Unknown' AS primary_service_type -- Placeholder: Replace with actual logic
    FROM customer_services cs
    LEFT JOIN services s ON cs.service_id = s.service_id -- Join if needed for service_type
    GROUP BY cs.customer_id
),

billing_agg AS (
    SELECT
        customer_id,
        AVG(CASE WHEN billing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) THEN amount_due END) AS avg_billing_amount_last_6m,
        SUM(CASE WHEN billing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) THEN amount_due ELSE 0 END) AS total_billed_amount_last_12m,
        COUNT(CASE WHEN payment_status = 'Late' AND billing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) THEN billing_id END) AS late_payment_count_last_12m,
        AVG(CASE WHEN payment_date IS NOT NULL AND billing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) THEN DATE_DIFF(payment_date, billing_date, DAY) END) AS avg_days_to_pay_last_6m
    FROM billing
    GROUP BY customer_id
),

support_tickets_agg AS (
    SELECT
        customer_id,
        COUNT(DISTINCT ticket_id) AS total_support_tickets,
        COUNT(DISTINCT CASE WHEN ticket_status = 'Open' THEN ticket_id END) AS open_support_tickets_count,
        COUNT(DISTINCT CASE WHEN ticket_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) THEN ticket_id END) AS support_tickets_last_3m,
        AVG(CASE WHEN ticket_resolution_date IS NOT NULL THEN TIMESTAMP_DIFF(ticket_resolution_date, ticket_creation_date, DAY) END) AS avg_ticket_resolution_time_days,
        MAX(ticket_creation_date) AS last_support_ticket_date
    FROM support_tickets
    GROUP BY customer_id
),

usage_agg AS (
    SELECT
        customer_id,
        AVG(CASE WHEN usage_month >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH) THEN data_usage_gb END) AS avg_monthly_data_usage_last_6m,
        AVG(CASE WHEN usage_month >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH) THEN call_minutes END) AS avg_monthly_call_minutes_last_6m,
        AVG(CASE WHEN usage_month >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH) THEN sms_count END) AS avg_monthly_sms_count_last_6m
    FROM usage
    GROUP BY customer_id
),

marketing_interactions_agg AS (
    SELECT
        customer_id,
        COUNT(DISTINCT CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) THEN interaction_id END) AS total_marketing_interactions_last_12m,
        SAFE_DIVIDE(
            SUM(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND response = TRUE THEN 1 ELSE 0 END),
            COUNT(DISTINCT CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) THEN interaction_id END)
        ) AS marketing_response_rate_last_12m,
        MAX(interaction_date) AS last_interaction_date
    FROM marketing_interactions
    GROUP BY customer_id
),

final AS (
    SELECT
        -- Grain
        c.customer_id,

        -- Attributes from Customers
        c.anonymized_demographics,
        c.anonymized_location,
        c.join_date,

        -- Attributes from Accounts
        a.account_type,
        a.contract_term,
        a.preferred_payment_method,

        -- Attribute from Services (derived via customer_services_agg)
        cs_agg.primary_service_type, -- Ensure logic in cs_agg CTE is correct

        -- Calculated Metrics
        DATE_DIFF(CURRENT_DATE(), c.join_date, DAY) AS tenure_days,
        COALESCE(cs_agg.active_services_count, 0) AS active_services_count,
        COALESCE(cs_agg.total_monthly_charge_active_services, 0) AS total_monthly_charge,
        cs_agg.avg_monthly_charge_active_services AS avg_monthly_charge_active,
        b_agg.avg_billing_amount_last_6m,
        COALESCE(b_agg.total_billed_amount_last_12m, 0) AS total_billed_amount_last_12m,
        COALESCE(b_agg.late_payment_count_last_12m, 0) AS late_payment_count_last_12m,
        b_agg.avg_days_to_pay_last_6m,
        COALESCE(st_agg.total_support_tickets, 0) AS total_support_tickets,
        COALESCE(st_agg.open_support_tickets_count, 0) AS open_support_tickets_count,
        COALESCE(st_agg.support_tickets_last_3m, 0) AS support_tickets_last_3m,
        st_agg.avg_ticket_resolution_time_days,
        u_agg.avg_monthly_data_usage_last_6m,
        u_agg.avg_monthly_call_minutes_last_6m,
        u_agg.avg_monthly_sms_count_last_6m,
        COALESCE(mi_agg.total_marketing_interactions_last_12m, 0) AS total_marketing_interactions_last_12m,
        mi_agg.marketing_response_rate_last_12m,
        DATE_DIFF(CURRENT_DATE(), mi_agg.last_interaction_date, DAY) AS days_since_last_interaction,
        DATE_DIFF(CURRENT_DATE(), st_agg.last_support_ticket_date, DAY) AS days_since_last_support_ticket,
        CASE WHEN c.status = 'Churned' THEN 1 ELSE 0 END AS Label,

        -- Metadata for incremental loads
        CURRENT_TIMESTAMP() as dbt_updated_at

    FROM customers c
    LEFT JOIN accounts a ON c.customer_id = a.customer_id
    LEFT JOIN customer_services_agg cs_agg ON c.customer_id = cs_agg.customer_id
    LEFT JOIN billing_agg b_agg ON c.customer_id = b_agg.customer_id
    LEFT JOIN support_tickets_agg st_agg ON c.customer_id = st_agg.customer_id
    LEFT JOIN usage_agg u_agg ON c.customer_id = u_agg.customer_id
    LEFT JOIN marketing_interactions_agg mi_agg ON c.customer_id = mi_agg.customer_id

    {% if is_incremental() %}
    -- Filter condition for incremental update based on unique key
    -- This assumes the 'customers' CTE is appropriately filtered for incremental runs
    -- or that changes in any source table related to a customer should trigger an update.
    -- A more robust incremental logic might involve checking update timestamps in all source CTEs.
    WHERE c.customer_id IN (SELECT customer_id FROM customers)
    {% endif %}
)

SELECT * FROM final
