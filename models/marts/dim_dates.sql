with
    date_series as (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2004-01-01' as date)",
            end_date="date_add(current_date, interval 20 year)"
            )
        }}
    )
    , date_columns as (
        select distinct
            cast(date_day as date) as metric_date
            , extract(day from date_day) as dayofmonth
            , extract(month from date_day) as month
            , extract(year from date_day) as year
            , extract(quarter from date_day) as quarter
            , extract(dayofyear from date_day) as dayofyear
            , extract(week from date_day) as isoweek
            , case
                when extract(dayofweek from date_day) = 1 then 'Sunday'
                when extract(dayofweek from date_day) = 2 then 'Monday'
                when extract(dayofweek from date_day) = 3 then 'Tuesday'
                when extract(dayofweek from date_day) = 4 then 'Wednesday'
                when extract(dayofweek from date_day) = 5 then 'Thursday'
                when extract(dayofweek from date_day) = 6 then 'Friday'
                when extract(dayofweek from date_day) = 7 then 'Saturday'
            end as dayofweek
        from date_series
    )
select *
from date_columns