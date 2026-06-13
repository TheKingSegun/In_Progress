{% snapshot snp_customers %}

{{
    config(
        target_schema = 'snapshots',
        strategy = 'timestamp',
        unique_key = 'customer_id',
        updated_at = 'customer_updated_at',
        invalidate_hard_deletes = True
    )
}}

/*
  SCD Type 2 snapshot for the customer dimension.

  Captures historical changes to customer attributes so we can answer:
  "What tier was this customer in when they placed this order?"
  "When did they first opt in to marketing?"

  This feeds dim_customers_history which the CRM and A/B testing attribution
  systems use for accurate historical segmentation.

  dbt manages the dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to
  columns automatically.
*/

select
    customer_id,
    email,
    first_name,
    last_name,
    country_code,
    customer_tier,
    acquisition_channel,
    is_email_verified,
    is_marketing_opted_in,
    date_of_birth,
    customer_created_at,
    customer_updated_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
