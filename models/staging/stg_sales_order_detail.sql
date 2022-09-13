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

, transformed as (
    select
    salesorderid		
        , salesorderdetailid			
        , orderqty				
        , productid				
        , unitprice			
        , unitpricediscount
        , (unitprice - unitpricediscount)*orderqty as subtotal_calculated
    from source_data
)

select *
from transformed