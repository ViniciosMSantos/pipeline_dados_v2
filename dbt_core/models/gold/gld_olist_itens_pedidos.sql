{{
    config(
        materialized='table',
        schema='gold',
        alias='gld_olist_itens_pedidos'
)
}}
select
    itens_pedidos.id_pedido,
    itens_pedidos.id_item,
    itens_pedidos.id_produto,
    itens_pedidos.id_vendedor,
    itens_pedidos.preco_produto,
    itens_pedidos.preco_frete,
    round(itens_pedidos.preco_produto + itens_pedidos.preco_frete, 2) as preco_total_produto,
    produtos.categoria_produto,
    vendedores.cidade_vendedor,
    vendedores.estado_vendedor
    
    
from {{ ref('slv_olist_itens_pedidos') }} as itens_pedidos

    left join {{ ref('slv_olist_produtos') }} as produtos
        on itens_pedidos.id_produto = produtos.id_produto

    left join {{ ref('slv_olist_vendedores') }} as vendedores
        on itens_pedidos.id_vendedor = vendedores.id_vendedor
    