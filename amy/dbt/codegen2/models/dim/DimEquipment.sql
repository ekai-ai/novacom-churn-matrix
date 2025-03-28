{{ config(materialized='incremental', unique_key='EquipmentSK') }}

with source as (
    select
        EQUIPMENT_ID,
        EQUIPMENT_TYPE,
        MODEL,
        SERIAL_NUMBER,
        FIRMWARE_VERSION,
        LOCATION,
        STATUS,
        LAST_MAINTENANCE_DATE
    from {{ ref('stg_nwk__equipment') }}
    where EQUIPMENT_ID is not null
)

select
    /* Generate surrogate key using farm_fingerprint on a concatenation of key fields */
    farm_fingerprint(cast(EQUIPMENT_ID as string) || TRIM(SERIAL_NUMBER)) as EquipmentSK,
    EQUIPMENT_ID as EquipmentID,
    UPPER(TRIM(EQUIPMENT_TYPE)) as EquipmentType,
    TRIM(MODEL) as Model,
    TRIM(SERIAL_NUMBER) as SerialNumber,
    TRIM(FIRMWARE_VERSION) as FirmwareVersion,
    TRIM(LOCATION) as Location,
    UPPER(TRIM(STATUS)) as Status,
    CAST(LAST_MAINTENANCE_DATE as DATE) as LastMaintenanceDate,
    CURRENT_TIMESTAMP() as SCDStartDate,  -- Effective date of the record version
    CAST('9999-12-31' as TIMESTAMP) as SCDEndDate,  -- Expiration date for current records
    TRUE as SCDIsCurrent  -- Flag indicating the current active record version
from source

{% if is_incremental() %}
  -- Additional logic for handling NEW_RECORD or CHANGED_RECORD_SCD2 should be implemented here
{% endif %}
