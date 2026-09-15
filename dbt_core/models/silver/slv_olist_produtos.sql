{{
config(
    materialized='table',
    schema='silver',
    alias='slv_olist_produtos'
)
}}

select
    product_id as id_produto,
    initcap(
        regexp_replace(
            product_category_name, '_', ' ')) 
    as categoria_produto,
    product_name_lenght as tamanho_nome_produto,
    product_description_lenght as tamanho_descricao_produto,
    product_photos_qty as quantidade_fotos_produto,
    product_weight_g as peso_produto,
    product_length_cm as comprimento_produto,
    product_height_cm as altura_produto,
    product_width_cm as largura_produto

from {{ source('bronze', 'brz_olist_products') }}