{{ config(materialized='table') }}

with source_data as (
    select
        USAGE_ID,
        ASSIGNMENT_ID,
        USAGE_DATE as USAGE_DATE_KEY,
        USAGE_TYPE,
        round(DATA_CONSUMED, 2) as DATA_CONSUMED,
        USAGE_UNIT,
        USAGE_PEAK,
        USAGE_COST
    from {{ ref('stg_nwk_usage') }}
    where USAGE_ID IS NOT NULL
      and ASSIGNMENT_ID IS NOT NULL
      and USAGE_DATE IS NOT NULL
      and USAGE_TYPE in ('Upload', 'Download', 'Total')
      and DATA_CONSUMED >= 0
      and USAGE_UNIT in ('KB', 'MB', 'GB', 'TB')
      and USAGE_PEAK >= 0
      and USAGE_COST >= 0
)

select *
from source_data;