with
    validation_test as (
        select orderqty
        from {{ ref ('fact_orders')}}
    )

select * 
from validation_test
where orderqty <0