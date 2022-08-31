with source_data as (
    select
        customerid				
        , personid				
        , storeid				
        , territoryid						
    from{{ source('advworks_erp', 'sales_customer') }}

)

select *
from source_data