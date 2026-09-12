{{config(
        materialized='table',
        schema='silver'
) }}

SELECT 
    order_id AS id_pedido,
    customer_id AS id_cliente,
    CASE 
        WHEN order_status = 'canceled'      THEN 'Cancelado'
        WHEN order_status = 'invoiced'      THEN 'Faturado'
        WHEN order_status = 'processing'    THEN 'Processando'
        WHEN order_status = 'shipped'       THEN 'Enviado'
        WHEN order_status = 'approved'      THEN 'Aprovado'
        WHEN order_status = 'created'       THEN 'Criado'
        WHEN order_status = 'unavailable'   THEN 'Indisponível'
        WHEN order_status = 'delivered'     THEN 'Entregue'
        ELSE 'Não informado'
    END AS status_pedido,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS data_pedido,
    CAST(order_approved_at AS TIMESTAMP) AS data_aprovacao_pedido,
    CAST(order_delivered_carrier_date AS TIMESTAMP) AS data_entrega_transportadora,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS data_entrega_cliente,
    CAST(order_estimated_delivery_date AS TIMESTAMP) AS data_estimada_entrega

FROM source('bronze', 'brz_olist_orders')
