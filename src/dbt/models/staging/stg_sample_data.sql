{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('raw', 'sample_data') }}
),

staged as (
    select
        id,
        product_name,
        price,
        quantity,
        priority,
        created_at,
        ingestion_timestamp,
        batch_id,
        source_system
    from source
)

select * from staged 