{{
config(
    materialized='table',
    schema='gold',
    alias='gold_olist_avaliacoes_deduplicado'
)
}}

select 
    *
from {{ ref('slv_olist_avaliacao') }}
    qualify 
        row_number() over (
            partition by 
                id_pedido 
            order by 
                data_avaliacao, 
                data_resposta_avaliacao, 
                id_avaliacao desc
        ) = 1