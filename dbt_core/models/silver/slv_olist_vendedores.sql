{{
config(
    materialized='table',
    schema='silver',
    alias='slv_olist_vendedores'
)
}}

select
    seller_id as id_vendedor,
    seller_zip_code_prefix as cep_vendedor,
    initcap(seller_city) as cidade_vendedor,
    seller_state as estado_vendedor

from {{ source('bronze', 'brz_olist_sellers') }}