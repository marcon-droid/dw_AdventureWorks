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
        row_number () over (order by customerid) as customerid_sk -- auto-incremental surrogate key									
        , customerid
        , persontype				
        , title				
        , firstname				
        , middlename				
        , lastname
        , concat(firstname, " ", middlename, " ", lastname)	as full_name
    from customers
    left join sales_person on customers.personid = sales_person.person_id
    and customers.personid is not null
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