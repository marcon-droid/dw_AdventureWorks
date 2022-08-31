with source_data as (
    select    	
        productid
        , productsubcategoryid										
        , 'name' as product_name					
        , productnumber					
        , class					
        , style					
        , productmodelid					
    from{{ source('advworks_erp', 'production_product') }}

)

select *
from source_data