{{ config(materialized='table', schema='dimensions') }}

WITH source AS (
    SELECT
        *
    FROM
        {{ ref('stg_prv_services') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['SERVICE_ID']) }} AS ServiceSK,
        SERVICE_ID,
        TRIM(SERVICE_NAME) AS SERVICE_NAME,
        SERVICE_TYPE,
        DESCRIPTION,
        MONTHLY_COST,
        ACTIVE_FLAG,
        PROVISIONING_TEAM
    FROM
        source
    WHERE
        SERVICE_ID IS NOT NULL
        AND SERVICE_TYPE IN ('Internet', 'Voice', 'TV', 'Mobile')
        AND MONTHLY_COST >= 0
        AND ACTIVE_FLAG IN (TRUE, FALSE)
)

SELECT
    *
FROM
    final