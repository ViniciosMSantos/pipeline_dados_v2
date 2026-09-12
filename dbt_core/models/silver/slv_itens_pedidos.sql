{{config(
        materialized='table'
) }}

SELECT
    order_id as id_pedido,
    order_item_id as id_item,
    product_id as id_produto,
    seller_id as id_vendedor,
    cast(shipping_limit_date as timestamp) as prazo_entrega,
    cast(price as double) as preco_produto,
    cast(freight_value as double) as preco_frete

FROM {{ source('bronze', 'brz_olist_order_items') }}