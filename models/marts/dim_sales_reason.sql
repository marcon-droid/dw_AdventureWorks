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
        row_number () over (order by salesreason.salesreasonid) as salesreason_sk
        , headersalesreason.salesorderid				
        , headersalesreason.salesreasonid				
        , salesreason.reason_name				
        , salesreason.reasontype
    from headersalesreason
    left join salesreason on headersalesreason.salesreasonid = salesreason.salesreasonid

)

select *
from joining