{{ config(materialized='table', schema='staging') }}

with raw_equipment as (
    select
        EQUIPMENT_ID,
        ASSIGNMENT_ID,
        EQUIPMENT_TYPE,
        MODEL,
        SERIAL_NUMBER,
        FIRMWARE_VERSION,
        IP_ADDRESS,
        MAC_ADDRESS,
        LOCATION,
        STATUS,
        LAST_MAINTENANCE_DATE,
        CREATED_AT,
        UPDATED_AT
    from {{ source('raw', 'NWK_EQUIPMENT') }}
)

select *
from raw_equipment;