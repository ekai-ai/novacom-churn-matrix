with source as (

    select * from {{ source('raw', 'crm_customers') }}

),

renamed as (

    select
        -- ids
        customer_id,

        -- strings
        first_name,
        last_name,
        email,
        customer_segment,
        customer_tier,
        status,

        -- timestamps
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at

    from source

)

select * from renamed