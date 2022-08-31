with sales_person as (
    select *
    from {{ ref('stg_person')}}
)

, customers as (
    select 
        customerid				
        , personid				
        , storeid				
        , territoryid
    from {{ ref('stg_customer') }}
)

, transformed as (
    select
        businessentityid as businessentity_sk -- auto-incremental surrogate key									
        , persontype				
        , title				
        , firstname				
        , middlename				
        , lastname	
    from sales_person
    left join customers on sales_person.businessentityid = customers.customerid
)

select *
from transformed


/*
select count(distinct businessentity_sk)
from transformed;



select count(distinct businessentityid)
from {{ ref('stg_person')}};

total = 19972


select count(distinct customerid)
from {{ ref('stg_customer')}}

total = 19820
*/