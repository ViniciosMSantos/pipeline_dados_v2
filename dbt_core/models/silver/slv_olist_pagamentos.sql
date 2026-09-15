{{
config(
    materialized='table',
    schema='silver',
    alias='slv_olist_pagamentos'
)
}}


select
    order_id as id_pedido,
    case 
        when payment_type = 'credit_card' then 'Cartão de crédito'
        when payment_type = 'boleto' then 'Boleto'
        when payment_type = 'voucher' then 'Vale'
        when payment_type = 'debit_card' then 'Cartão de débito'
        else 'Outros'
    end as tipo_pagamento,
    payment_installments as parcelas_pagamento,
    cast(payment_value as double) as valor_pagamento,
    payment_sequential as sequencia_pagamento

from dados_prod.bronze.brz_olist_order_payments