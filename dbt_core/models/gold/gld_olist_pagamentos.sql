{{
    config(
        materialized='table',
        schema='gold',
        alias='gld_olist_pagamentos'
    )
}}

select * from {{ ref('slv_olist_pagamentos') }} 
