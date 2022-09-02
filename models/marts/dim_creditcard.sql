with creditcard as (
    select *
    from {{ ref('stg_creditcard')}}
)

, transformed as (
    select
       row_number () over (order by creditcardid) as creditcard_sk -- auto-incremental surrogate key				
        , creditcardid
        , cardtype				
        , cardnumber 	
    from creditcard
)

select *
from transformed