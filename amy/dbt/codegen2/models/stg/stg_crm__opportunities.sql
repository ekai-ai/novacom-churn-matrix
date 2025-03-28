{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'CRM_OPPORTUNITIES') }}

),

renamed as (

    select
        OPPORTUNITY_ID,
        ACCOUNT_ID,
        STAGE,
        PIPELINE_STAGE,
        AMOUNT,
        PROBABILITY_OF_CLOSE,
        CLOSE_DATE,
        ASSIGNED_SALES_REP,
        NEXT_STEP,
        NOTE,
        CREATED_AT,
        UPDATED_AT

    from source

)

select * from renamed