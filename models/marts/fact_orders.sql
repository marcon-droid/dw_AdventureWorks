with sales_address as (
    select addressid_sk
    , addressid
    from {{ref('dim_address')}}
)

, creditcard as (
    select creditcard_sk
    , creditcardid
    from {{ref('dim_creditcard')}}
)

, products as (
    select productid_sk
    , productid
    from {{ref('dim_products')}}
)

, customers as (
    select customerid_sk
    , person_id
    from {{ref('dim_sales_customers')}}
)

, reason as (
    select salesreason_sk
    , salesreasonid
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
    from {{ref('stg_sales_order_header')}} as sales_order
    left join customers on sales_order.customerid = customers.person_id
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
        , productsubcategoryid												
        , salesorderdetailid			
        , orderqty				
        , productid				
        , unitprice			
        , unitpricediscount
        , product_name
    from {{ref('stg_sales_order_detail')}} order_detail
    left join products on order_detail.productid = products.productid
)

/*
joining sales_reason on salesorderheadersalesreason
*/

, sales_reason_with_sk as (
    select
        salesorderid
        , salesreason_sk as salesreason_fk
    from {{ref('stg_sls_order_header_sls_reason')}} sales_order_header_reason
    left join reason on sales_order_header_reason.salesreasonid = reason.salesreasonid 
)

/* We then join orders and orders detail to get the final fact table*/
, final as (
    select
        order_dtl.order_id
        , orders.employee_fk
        , orders.customer_fk
        , orders.order_date
        , orders.ship_region
        , orders.shipped_date
        , orders.ship_country
        , orders.ship_name
        , orders.ship_postal_code
        , orders.ship_city
        , orders.freight
        , orders.ship_address
        , orders.required_date
        , order_dtl.product_fk
        , order_dtl.discount
        , order_dtl.unit_price
        , order_dtl.quantity
        , order_dtl.unit_price * order_dtl.quantity as gross_total
    from orders_with_sk as orders
    left join orders_detail_with_sk as order_dtl on orders.order_id = order_dtl.order_id
)

select * 
from final
