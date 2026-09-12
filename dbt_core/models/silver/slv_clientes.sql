{{ config(
    materialized='table'
) }}

SELECT
    customer_id as id_cliente,
    customer_unique_id as id_cliente_unico,
    customer_city as cidade_cliente,
    customer_state as estado_cliente
FROM {{ source('bronze', 'brz_olist_customers') }}