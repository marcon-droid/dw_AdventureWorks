with source_data as (
    select
        creditcardid				
        , cardtype				
        , cardnumber				
    from{{ source('advworks_erp', 'sales_creditcard') }}
)

select *
from source_data