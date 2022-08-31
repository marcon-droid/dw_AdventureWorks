with source_data as (
    select	
        salesreasonid				
        , 'name' as reason_name		
        , reasontype				
    from{{ source('advworks_erp', 'sales_salesreason') }}

)

select *
from source_data