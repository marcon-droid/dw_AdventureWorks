with source_data as (
    select        	
        businessentityid as person_id				
        , persontype				
        , title				
        , firstname				
        , middlename				
        , lastname
        , concat(firstname," ", lastname) as full_name
    from{{ source('advworks_erp', 'person_person') }}
)

select *
from source_data