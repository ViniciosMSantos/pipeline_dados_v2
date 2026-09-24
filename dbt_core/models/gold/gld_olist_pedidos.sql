{{
    config(
        materialized='table',
        schema='gold',
        alias='gld_olist_pedidos'
    )
}}

with pedidos as (
    select
        pedidos.id_pedido,
        clientes.id_cliente,
        clientes.id_cliente_unico,
        pedidos.status_pedido,
        round(sum(itens.preco_produto), 2) as valor_total_produtos,
        round(sum(itens.preco_frete), 2) as valor_total_frete,
        round(sum(itens.preco_produto) + sum(itens.preco_frete), 2) as valor_total_pedido,
        count(itens.id_item) as qtd_itens,
        to_date(pedidos.data_pedido) as data_pedido,
        pedidos.data_pedido as data_hora_pedido,
        to_date(pedidos.data_aprovacao_pedido) as data_aprovacao_pedido,
        to_date(pedidos.data_entrega_transportadora) as data_entrega_transportadora,
        to_date(pedidos.data_entrega_cliente) as data_entrega_cliente,
        to_date(pedidos.data_estimada_entrega) as data_estimada_entrega,
        initcap(clientes.cidade_cliente) as cidade_cliente,
        clientes.estado_cliente,
        timestampdiff(DAY, pedidos.data_pedido, pedidos.data_entrega_cliente) as dias_entrega,
        timestampdiff(DAY, pedidos.data_pedido, pedidos.data_estimada_entrega) as dias_previsao_entrega,
        timestampdiff(DAY, pedidos.data_estimada_entrega, pedidos.data_entrega_cliente) as dias_atraso,
        case
            when status_pedido = 'Entregue' then true 
            else false
        end as pedido_entregue,
        case 
            when status_pedido = 'Cancelado' then true 
            else false
        end as pedido_cancelado

    from {{ ref('slv_olist_pedidos') }} as pedidos
        left join {{ ref('slv_olist_clientes') }} as clientes
            on pedidos.id_cliente = clientes.id_cliente 
        left join {{ ref('slv_olist_itens_pedidos') }} as itens
            on pedidos.id_pedido = itens.id_pedido

                group by 
                        pedidos.id_pedido,
                        clientes.id_cliente,
                        clientes.id_cliente_unico,
                        pedidos.status_pedido,
                        pedidos.data_pedido,
                        pedidos.data_aprovacao_pedido,
                        pedidos.data_entrega_transportadora,
                        pedidos.data_entrega_cliente,
                        pedidos.data_estimada_entrega,
                        clientes.cidade_cliente,
                        clientes.estado_cliente

)

select
    id_pedido,
    id_cliente,
    id_cliente_unico,
    status_pedido,
    valor_total_produtos,
    valor_total_frete,
    valor_total_pedido,
    qtd_itens,
    data_pedido,
    data_hora_pedido,
    data_aprovacao_pedido,
    data_entrega_transportadora,
    data_entrega_cliente,
    data_estimada_entrega,
    cidade_cliente,
    estado_cliente,
    dias_entrega,
    dias_previsao_entrega,
    case 
        when dias_atraso > 0 
            then dias_atraso
        else null
    end as dias_atraso,
    pedido_entregue,
    pedido_cancelado,
    case 
        when dias_atraso > 0 
            then true
        else false
    end as pedido_entrega_atrasado

from pedidos
    where date_format(data_pedido, 'yyyy-MM-dd') <= '2018-08-29'  