with source_data as (
    select
        addressid			
        , stateprovinceid			
        , addressline1				
        , city				
        , postalcode
    from{{ source('advworks_erp', 'person_address') }}

)

select *
from source_data