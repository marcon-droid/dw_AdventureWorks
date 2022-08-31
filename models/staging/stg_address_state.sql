with source_data as (
    select
        stateprovinceid					
        , stateprovincecode					
        , countryregioncode					
        , 'name' as state_name					
        , territoryid					
    from{{ source('advworks_erp', 'person_stateprovince') }}

)

select *
from source_data