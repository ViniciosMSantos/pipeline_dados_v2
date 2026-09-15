{{
config(
    materialized='table',
    schema='silver',
    alias='slv_olist_cidades'
)
}}

with geo as (

    select
        geolocation_zip_code_prefix as cep_cidade,
        initcap(geolocation_city) as nome_cidade,
        geolocation_state as estado_cidade,
        geolocation_lat as latitude_cidade,
        geolocation_lng as longitude_cidade

    from {{ source('bronze', 'brz_olist_geolocation') }}

),

nome_mais_frequente as (

    select
        cep_cidade,
        nome_cidade,
        estado_cidade,
        row_number() over (
            partition by cep_cidade
            order by count(*) desc
        ) as rn

    from geo
    group by cep_cidade, nome_cidade, estado_cidade

)

select
    geo.cep_cidade,
    nomes.nome_cidade,
    nomes.estado_cidade,
    avg(geo.latitude_cidade) as latitude_cidade,
    avg(geo.longitude_cidade) as longitude_cidade

from geo
inner join nome_mais_frequente as nomes
    on geo.cep_cidade = nomes.cep_cidade
    and nomes.rn = 1

group by geo.cep_cidade, nomes.nome_cidade, nomes.estado_cidade