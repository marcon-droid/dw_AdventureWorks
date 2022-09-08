with salesreason as (
    select *
    from {{ ref("stg_sales_reason") }}
)

, joined as (
    select
        row_number () over (order by salesreason.salesreasonid) as salesreason_sk
        , salesreasonid
        , reason_name				
        , reasontype
    from salesreason
)

select *
from joined