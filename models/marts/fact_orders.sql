with sales_address as (
    select *
    from {{ref('dim_address')}}
)

, creditcard as (
    select *
    from {{ref('dim_creditcard')}}
)

, products as (
    select *
    from {{ref('dim_products')}}
)

, customers as (
    select *
    from {{ref('dim_sales_customers')}}
)

, reason as (
    select *
    from {{ref('dim_sales_reason')}}
)

/*
joining CUSTOMERS, CREDIT CARD, ADDRESS on sales_order_header
*/

, slsorderheader_with_sk as (
    select
        salesorderid
        , customers.customerid_sk as customerid_fk
        , creditcard.creditcard_sk as creditcard_fk				
        , sales_address.addressid_sk as addressid_fk
        , sales_order.orderdate				
        , sales_order.duedate					
        , sales_order.shipdate					
        , sales_order.order_status					
        , sales_order.subtotal					
        , sales_order.totaldue
        , customers.full_name
    from {{ref('stg_sales_order_header')}} as sales_order
    left join customers on sales_order.customerid = customers.customer_id
    left join creditcard on sales_order.creditcardid = creditcard.creditcardid
    left join sales_address on sales_order.billtoaddressid = sales_address.addressid
)

/*
joining PRODUCT  on sales_order_detail
*/

, orders_detail_with_sk as (
    select
        salesorderid
        , productid_sk as productid_fk
        , salesorderdetailid			
        , order_detail.orderqty				
        , order_detail.productid				
        , order_detail.unitprice			
        , order_detail.unitpricediscount
        , products.product_name
    from {{ref('stg_sales_order_detail')}} order_detail
    left join products on order_detail.productid = products.productid
)

/*
joining sales_reason on salesorderheadersalesreason
*/

, sales_reason_with_sk as (
    select
        row_number() over (partition by salesorderid order by sales_order_header_reason.salesreasonid) as dedup_index
        , salesorderid
        , salesreason_sk as salesreason_fk
    from {{ref('stg_sls_order_header_sls_reason')}} sales_order_header_reason
    left join reason on sales_order_header_reason.salesreasonid = reason.salesreasonid 
)

, dedup_salesreason_with_sk as (
    select 
        salesorderid
        , salesreason_fk
    from sales_reason_with_sk
    where dedup_index = 1
)

/* We then join orders and orders detail to get the final fact table*/
, final as (
    select
        slsorderheader_with_sk.salesorderid
        , slsorderheader_with_sk.customerid_fk
        , slsorderheader_with_sk.creditcard_fk
        , slsorderheader_with_sk.addressid_fk
        , slsorderheader_with_sk.orderdate				
        , slsorderheader_with_sk.duedate					
        , slsorderheader_with_sk.shipdate					
        , slsorderheader_with_sk.order_status					
        , slsorderheader_with_sk.subtotal					
        , slsorderheader_with_sk.totaldue
        , slsorderheader_with_sk.full_name
        , orders_detail_with_sk.productid_fk
        , orders_detail_with_sk.salesorderdetailid
        , orders_detail_with_sk.orderqty
        , orders_detail_with_sk.productid
        , orders_detail_with_sk.unitprice
        , orders_detail_with_sk.unitpricediscount
        , orders_detail_with_sk.product_name
        , dedup_salesreason_with_sk.salesreason_fk
    from slsorderheader_with_sk 
    left join orders_detail_with_sk on slsorderheader_with_sk.salesorderid = orders_detail_with_sk.salesorderid
    left join dedup_salesreason_with_sk on slsorderheader_with_sk.salesorderid = dedup_salesreason_with_sk.salesorderid
)

select *
from final
