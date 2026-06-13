{{
    config(
        materialized = 'table',
        tags = ['mart', 'core', 'customers'],
        post_hook = "analyze {{ this }}"
    )
}}

/*
  dim_customers — customer dimension with pre-computed LTV metrics.

  Joins the customer profile (from staging) with the Gold layer LTV output
  (loaded from S3 into Redshift by Airflow after the Gold Spark job).

  This is NOT an SCD — slowly-changing attributes (tier, address) are
  handled in the snapshot snp_customers.sql.  This model is the current-state
  dimension for BI dashboards that don't need historical values.
*/

with customers as (

    select * from {{ ref('stg_customers') }}

),

ltv as (

    select
        customer_id,
        total_orders,
        completed_orders,
        cancelled_orders,
        total_revenue_usd,
        avg_order_value_usd,
        first_order_date,
        last_order_date,
        days_since_last_order,
        revenue_l12m_usd,
        orders_l12m,
        ltv_segment,
        cancel_rate
    from {{ source('silver', 'customer_ltv_gold') }}

),

final as (

    select
        -- key
        c.customer_id,

        -- identity
        c.email,
        c.first_name,
        c.last_name,
        c.first_name || ' ' || c.last_name       as full_name,

        -- profile
        c.country_code,
        c.acquisition_channel,
        c.customer_tier,
        c.is_email_verified,
        c.is_marketing_opted_in,
        c.date_of_birth,

        -- LTV metrics (from Gold layer)
        coalesce(l.total_orders, 0)              as total_orders,
        coalesce(l.completed_orders, 0)          as completed_orders,
        coalesce(l.cancelled_orders, 0)          as cancelled_orders,
        coalesce(l.total_revenue_usd, 0)         as total_revenue_usd,
        l.avg_order_value_usd,
        l.first_order_date,
        l.last_order_date,
        l.days_since_last_order,
        coalesce(l.revenue_l12m_usd, 0)          as revenue_l12m_usd,
        coalesce(l.orders_l12m, 0)               as orders_l12m,
        coalesce(l.ltv_segment, 'new_customers') as ltv_segment,
        coalesce(l.cancel_rate, 0)               as cancel_rate,

        -- customer age in days (for cohort analysis)
        datediff('day', c.customer_created_at::date, current_date) as customer_age_days,

        -- derived flags
        case
            when l.first_order_date is null then true
            else false
        end                                      as is_never_purchased,

        -- dates
        c.customer_created_at,
        c.customer_updated_at,

        -- audit
        current_timestamp                        as _dbt_updated_at

    from customers c
    left join ltv l using (customer_id)

)

select * from final
