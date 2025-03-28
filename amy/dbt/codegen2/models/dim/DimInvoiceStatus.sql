{{ config(
    materialized='table'
) }}

WITH source_data AS (
    -- Select distinct non-null statuses from the source table
    SELECT DISTINCT
        STATUS
    FROM {{ ref('stg_bil__invoices') }}
    WHERE STATUS IS NOT NULL
),

transformed_data AS (
    -- Standardize the status names (UPPER, TRIM)
    SELECT
        UPPER(TRIM(STATUS)) AS InvoiceStatusName
    FROM source_data
),

final AS (
    -- Generate the surrogate key using the standardized status name
    SELECT
        {{ dbt_utils.generate_surrogate_key(['InvoiceStatusName']) }} AS InvoiceStatusSK,
        InvoiceStatusName
    FROM transformed_data
)

-- Select all columns from the final CTE
SELECT * FROM final