{{
config(
    materialized='table',
    schema='silver',
    alias='slv_olist_avaliacao'
)
}}


select
    review_id as id_avaliacao,
    order_id as id_pedido,
    review_score as nota_avaliacao,
    review_comment_title as titulo_avaliacao,
    review_comment_message as comentario_avaliacao,
    cast(review_creation_date as timestamp) as data_avaliacao,
    cast(review_answer_timestamp as timestamp) as data_resposta_avaliacao

from {{ source('bronze', 'brz_olist_order_reviews') }}