{{
config(
    materialized='table',
    schema='gold',
    alias='gld_olist_avaliacoes'
)
}}

select * from {{ ref('slv_olist_avaliacao') }}
