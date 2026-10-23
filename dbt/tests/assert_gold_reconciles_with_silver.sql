-- Row count and revenue in gold must equal silver for every month: nothing lost, nothing doubled.
with silver as (
    select pickup_month, count(*) as rows, round(sum(total_amount), 2) as revenue
    from {{ ref('stg_yellow_trips') }} group by 1
),
gold as (
    select pickup_month, count(*) as rows, round(sum(total_amount), 2) as revenue
    from {{ ref('fct_trips') }} group by 1
)
select coalesce(s.pickup_month, g.pickup_month) as pickup_month, s.rows as silver_rows, g.rows as gold_rows,
       s.revenue as silver_revenue, g.revenue as gold_revenue
from silver s full outer join gold g using (pickup_month)
where s.rows is distinct from g.rows or s.revenue is distinct from g.revenue
