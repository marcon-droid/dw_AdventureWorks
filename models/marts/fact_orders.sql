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
joining ORDER DETAIL,  on sales_order_header
*/

, orders_detail_with_sk as (
    select
        order_dtl.order_id
        , products.product_sk as product_fk
        , order_dtl.discount
        , order_dtl.unit_price
        , order_dtl.quantity
    from {{ref('stg_sales_order_detail')}} order_dtl
    left join products on order_dtl.product_id = products.product_id
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
