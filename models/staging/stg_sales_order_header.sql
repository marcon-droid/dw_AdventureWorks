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
        , cast(orderdate as date) as orderdate
        , extract(month from orderdate) as order_date_month
        , extract(year from orderdate) as order_date_year
        , duedate				
        , cast(shipdate as date) as shipdate 					
        , 'status' as order_status					
        , onlineorderflag					
        , purchaseordernumber					
        , subtotal					
        , totaldue
    from{{ source('advworks_erp', 'sales_salesorderheader') }}

)

select *
from source_data