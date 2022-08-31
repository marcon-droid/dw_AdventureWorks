with address_city as (
    select *
    from {{ ref("stg_address_city")}}
)

, address_state as (
    select *
    from {{ ref("stg_address_state") }}
)

, address_country as (
    select *
    from {{ ref("stg_address_country") }}
)

/* joining them to create address */

, joining as (
    select
        address_city.addressid as addressid_sk				
        , address_city.addressline1				
        , address_city.city				
        , address_city.stateprovinceid			
        , address_city.postalcode
        , address_state.stateprovincecode					
        , address_state.state_name 					
        , address_state.territoryid
        , address_country.countryregioncode
    from address_city
    left join address_state on address_city.stateprovinceid = address_state.stateprovinceid
    left join address_country on address_state.countryregioncode = address_country.countryregioncode

)

select *
from joining