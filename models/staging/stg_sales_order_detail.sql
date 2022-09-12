with source_data as (
    select	
        salesorderid		
        , salesorderdetailid			
        , orderqty				
        , productid				
        , cast (unitprice as numeric) as unitprice			
        , unitpricediscount			
    from{{ source('advworks_erp', 'salesorderdetail') }}

)

select *
from source_data