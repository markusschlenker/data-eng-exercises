with customers as (
    select
        product_id,
        product_category_name,
        product_weight_g,
        product_photos_qty
    from {{ source('olist', 'OLIST_PRODUCTS_DATASET') }}
)

select * from customers
