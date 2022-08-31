with source_data as (
    select   	
        salesorderid					
        , salesreasonid					
        , modifieddate		
    from{{ source('advworks_erp', 'salesorderheadersalesreason') }}

)

select *
from source_data