with
    test_orders as (
        select count (distinct salesorderid) as orderid_summed
        from {{ ref ('fact_orders')}}
        where orderdate between '2011-05-31' and '2012-05-31'
    )

select * 
from test_orders 
where orderid_summed != 3028