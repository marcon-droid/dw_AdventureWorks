with source_data as (
    select
        countryregioncode				
        , 'name' as country_name			
    from{{ source('advworks_erp', 'person_countryregion') }}
)

select *
from source_data