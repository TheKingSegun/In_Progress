{{
    config(
        materialized = 'table',
        tags = ['mart', 'finance'],
        meta = {
            'owner': 'data-engineering',
            'stakeholders': ['finance', 'exec'],
            'sla': 'T+2h'
        }
    )
}}

/*
  revenue_summary — daily revenue aggregation for the finance dashboard.

  This model is the source of truth for the P&L revenue line.
  It intentionally does NOT include order-level detail — that's fct_orders.

  Grain: one row per (order_date, channel, customer_tier, country_code).

  Metrics:
    - gross_revenue_usd:     Sum of all order amounts (including cancelled)
    - net_revenue_usd:       Gross minus refunds/cancellations
    - recognised_revenue_usd: Net, shipped/delivered only (accounting view)
    - cancellation_rate:     Pct of orders that were cancelled/refunded
    - aov_usd:               Average order value (completed orders)

  Finance team owns the definitions here — changes must go through their
  review process in addition to standard data engineering review.
*/

with orders as (

    select * from {{ ref('fct_orders') }}

),

daily_summary as (

    select
        order_date,
        channel,
        customer_tier,
        shipping_country                         as country_code,
        revenue_band,

        -- Volume
        count(order_id)                          as total_orders,
        count(case when order_status = 'delivered' then 1 end) as delivered_orders,
        count(case when order_status in ('cancelled', 'refunded', 'returned')
                   then 1 end)                   as cancelled_orders,
        sum(item_count)                          as total_items_sold,

        -- Revenue
        sum(order_total_usd)                     as gross_revenue_usd,

        sum(recognised_revenue_usd)              as recognised_revenue_usd,

        sum(lost_revenue_usd)                    as lost_revenue_usd,

        sum(order_total_usd)
            - sum(lost_revenue_usd)              as net_revenue_usd,

        -- Averages
        avg(case when order_status in ('delivered', 'shipped')
                 then order_total_usd end)       as aov_usd,

        -- Rates
        round(
            1.0 * count(case when order_status in ('cancelled', 'refunded', 'returned')
                             then 1 end)
            / nullif(count(order_id), 0),
            4
        )                                        as cancellation_rate,

        -- High-value order share
        round(
            1.0 * count(case when is_high_value then 1 end)
            / nullif(count(order_id), 0),
            4
        )                                        as high_value_order_pct,

        -- International share
        round(
            1.0 * count(case when is_international then 1 end)
            / nullif(count(order_id), 0),
            4
        )                                        as international_order_pct,

        current_timestamp                        as _dbt_updated_at

    from orders
    group by 1, 2, 3, 4, 5

)

select * from daily_summary
