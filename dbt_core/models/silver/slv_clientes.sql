{{ config(
    materialized='table'
) }}

SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM {{ source('bronze', 'brz_olist_customers') }}