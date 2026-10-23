-- Daily demand and revenue per pickup zone: the table analysts and dashboards read.
select
    t.pickup_date,
    t.pickup_zone_id,
    z.borough,
    z.zone_name,
    count(*)                                              as trips,
    round(sum(t.total_amount), 2)                         as revenue,
    round(avg(t.fare_amount), 2)                          as avg_fare,
    round(median(t.trip_minutes), 1)                      as median_trip_minutes,
    round(sum(t.tip_amount) filter (where t.payment_type_id = 1)
          / nullif(sum(t.fare_amount) filter (where t.payment_type_id = 1), 0), 4) as card_tip_rate
from {{ ref('fct_trips') }} t
join {{ ref('dim_zone') }} z on z.zone_id = t.pickup_zone_id
group by all
