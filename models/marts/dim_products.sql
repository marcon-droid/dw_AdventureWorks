with products as (
    select *
    from {{ ref('stg_product')}}
)

, transformed as (
    select
        row_number () over (order by productid) as productid_sk 
        , productid
        , productsubcategoryid										
        , product_name
    from products
)

select *
from transformed