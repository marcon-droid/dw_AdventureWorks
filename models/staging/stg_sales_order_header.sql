with source_data as (
    select 
        salesorderid
        , customerid					
        , salespersonid					
        , territoryid					
        , billtoaddressid					
        , shiptoaddressid					
        , shipmethodid					
        , creditcardid					
        , creditcardapprovalcode					
        , currencyrateid					
        , orderdate				
        , duedate					
        , shipdate					
        , 'status' as order_status					
        , onlineorderflag					
        , purchaseordernumber					
        , subtotal					
        , totaldue
    from{{ source('advworks_erp', 'sales_salesorderheader') }}

)

select *
from source_data