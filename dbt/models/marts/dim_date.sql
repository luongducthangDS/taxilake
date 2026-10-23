with spine as (
    select unnest(generate_series(date '2020-01-01', date '2021-12-31', interval 1 day))::date as date_day
)
select
    date_day,
    year(date_day)                           as year,
    month(date_day)                          as month,
    date_trunc('month', date_day)::date      as month_start,
    dayname(date_day)                        as day_name,
    isodow(date_day)                         as iso_day_of_week,
    isodow(date_day) in (6, 7)               as is_weekend
from spine
