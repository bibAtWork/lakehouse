{{
  config(
    materialized='table'
  )
}}

with staged_data as (
    select * from {{ ref('stg_sample_data') }}
),

product_summary as (
    select
        product_name,
        priority,
        count(*) as total_records,
        sum(quantity) as total_quantity,
        sum(price * quantity) as total_revenue,
        avg(price) as avg_price,
        min(price) as min_price,
        max(price) as max_price,
        current_timestamp() as created_at
    from staged_data
    group by product_name, priority
)

select * from product_summary 