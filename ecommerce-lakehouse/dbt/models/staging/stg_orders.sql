{{
    config(
        materialized = 'view',
        tags = ['staging', 'orders']
    )
}}

/*
  Staging model for orders.

  Responsibilities:
    - Rename columns to a consistent snake_case convention
    - Cast to canonical types (Silver has strings for flexibility; here we enforce)
    - Surface only the columns downstream models should use
    - No business logic — that belongs in intermediate or marts

  Note: order_total_usd is already converted from the source currency by the
  Silver PySpark job.  We expose the original currency for transparency.
*/

with source as (

    select * from {{ source('silver', 'orders') }}

),

staged as (

    select
        -- keys
        order_id,
        customer_id,

        -- order attributes
        order_status,
        order_total_usd::decimal(18, 2)         as order_total_usd,
        order_total_amount::decimal(18, 4)       as order_total_original,
        currency_code,
        channel,
        shipping_country,
        item_count::int                          as item_count,

        -- flags derived by Silver PySpark
        is_high_value,
        order_size_tier,
        is_international,

        -- dates
        order_date::date                         as order_date,
        order_created_at::timestamp              as order_created_at,
        order_updated_at::timestamp              as order_updated_at,

        -- audit
        _cdc_operation,
        _ingested_at

    from source

)

select * from staged
