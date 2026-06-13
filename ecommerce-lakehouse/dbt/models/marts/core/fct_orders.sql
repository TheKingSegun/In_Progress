{{
    config(
        materialized = 'incremental',
        unique_key = 'order_id',
        incremental_strategy = 'merge',
        cluster_by = ['order_date'],
        tags = ['mart', 'core', 'orders']
    )
}}

/*
  fct_orders — the central fact table for order analytics.

  Grain: one row per order (post-deduplication).

  Incremental strategy:
    Merge on order_id allows idempotent reruns.  We look back 3 days in
    incremental mode to catch late-arriving CDC events (orders updated after
    the initial ingest window, e.g. delayed delivery confirmations).

  Downstream consumers: BI dashboards, finance reporting, CRM exports,
  the A/B test attribution pipeline.
*/

with source as (

    select * from {{ ref('int_orders_enriched') }}

    {% if is_incremental() %}
    -- Look back 3 days to capture late-arriving updates
    where order_date >= (
        select dateadd('day', -3, max(order_date)) from {{ this }}
    )
    {% endif %}

),

final as (

    select
        -- surrogate key (already a UUID from source — no hashing needed)
        order_id,

        -- foreign keys
        customer_id,

        -- order attributes
        order_status,
        channel,
        shipping_country,
        customer_country,
        acquisition_channel,
        customer_tier,
        revenue_band,
        order_size_tier,
        item_count,

        -- financials
        order_total_usd,
        order_total_original,
        currency_code,

        -- boolean flags
        is_high_value,
        is_international,
        is_cross_border,

        -- revenue recognition
        -- Only count delivered/shipped as recognised revenue
        case
            when order_status in ('delivered', 'shipped')
            then order_total_usd
            else 0
        end                                      as recognised_revenue_usd,

        case
            when order_status in ('cancelled', 'refunded', 'returned')
            then order_total_usd
            else 0
        end                                      as lost_revenue_usd,

        -- dates
        order_date,
        order_created_at,
        order_updated_at,
        days_since_customer_signup,

        -- audit
        _ingested_at                             as _loaded_at,
        current_timestamp                        as _dbt_updated_at

    from source

)

select * from final
