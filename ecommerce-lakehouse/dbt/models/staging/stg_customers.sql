{{
    config(
        materialized = 'view',
        tags = ['staging', 'customers']
    )
}}

with source as (

    select * from {{ source('silver', 'customers') }}

),

staged as (

    select
        customer_id,
        lower(trim(email))                       as email,
        coalesce(first_name, 'Unknown')          as first_name,
        coalesce(last_name, '')                  as last_name,

        -- normalise country to ISO-3166 alpha-2
        upper(trim(country_code))                as country_code,
        lower(trim(acquisition_channel))         as acquisition_channel,

        -- membership
        customer_tier,
        is_email_verified::boolean               as is_email_verified,
        is_marketing_opted_in::boolean           as is_marketing_opted_in,

        -- dates
        created_at::timestamp                    as customer_created_at,
        updated_at::timestamp                    as customer_updated_at,
        date_of_birth::date                      as date_of_birth,

        -- audit
        _ingested_at

    from source

)

select * from staged
