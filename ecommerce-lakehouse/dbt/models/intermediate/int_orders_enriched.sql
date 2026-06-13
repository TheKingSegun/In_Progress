{{
    config(
        materialized = 'ephemeral',
        tags = ['intermediate']
    )
}}

/*
  Joins orders with customer context and derives additional columns.
  Ephemeral means this is inlined as a CTE — no table/view created.
  Downstream models that need this enriched version simply reference it.
*/

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select
        customer_id,
        customer_tier,
        country_code         as customer_country,
        acquisition_channel,
        customer_created_at
    from {{ ref('stg_customers') }}

),

enriched as (

    select
        o.*,

        -- Customer context at order time
        c.customer_tier,
        c.customer_country,
        c.acquisition_channel,

        -- Days between customer signup and first touchpoint (cohort analysis)
        datediff(
            'day',
            c.customer_created_at::date,
            o.order_date
        )                                        as days_since_customer_signup,

        -- Whether the order was placed in the same country as the customer's
        -- registered country (cross-border purchasing indicator)
        case
            when o.shipping_country = c.customer_country then false
            else true
        end                                      as is_cross_border,

        -- Revenue band — used for cohort bucketing in finance mart
        case
            when o.order_total_usd < 25    then 'micro'
            when o.order_total_usd < 100   then 'small'
            when o.order_total_usd < 500   then 'medium'
            when o.order_total_usd < 2000  then 'large'
            else 'enterprise'
        end                                      as revenue_band

    from orders o
    left join customers c using (customer_id)

)

select * from enriched
