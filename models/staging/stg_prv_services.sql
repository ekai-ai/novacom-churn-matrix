WITH source AS (
    SELECT
        service_id,
        service_name,
        service_type,
        description,
        monthly_cost,
        provisioning_team,
        active_flag,
        created_date,
        updated_at
    FROM {{ source('raw', 'prv_services') }}
),

renamed_casted AS (
    SELECT
        CAST(service_id AS VARCHAR) AS service_id,
        CAST(service_name AS VARCHAR) AS service_name,
        CAST(service_type AS VARCHAR) AS service_type,
        CAST(description AS VARCHAR) AS description,
        CAST(monthly_cost AS NUMERIC(18, 2)) AS monthly_cost, -- Assuming standard precision/scale for currency
        CAST(provisioning_team AS VARCHAR) AS provisioning_team,
        CAST(active_flag AS BOOLEAN) AS active_flag,
        CAST(created_date AS DATE) AS created_date,
        CAST(updated_at AS TIMESTAMP) AS updated_at
    FROM source
)

SELECT
    service_id,
    service_name,
    service_type,
    description,
    monthly_cost,
    provisioning_team,
    active_flag,
    created_date,
    updated_at
FROM renamed_casted