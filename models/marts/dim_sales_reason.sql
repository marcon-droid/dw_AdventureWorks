with headersalesreason as (
    select *
    from {{ ref("stg_sls_order_header_sls_reason")}}
)

, salesreason as (
    select *
    from {{ ref("stg_sales_reason") }}
)

, joining as (
    select
        headersalesreason.salesorderid as salesID_sk				
        , headersalesreason.salesreasonid				
        , headersalesreason.modifieddate
        , salesreason.reason_name				
        , salesreason.reasontype
    from headersalesreason
    left join salesreason on headersalesreason.salesreasonid = salesreason.salesreasonid

)

select *
from joining