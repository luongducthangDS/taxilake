-- The daily aggregate must add up to the fact table.
select 'trips' as metric
where (select sum(trips) from {{ ref('agg_daily_zone') }}) <> (select count(*) from {{ ref('fct_trips') }})
union all
select 'revenue'
where abs((select sum(revenue) from {{ ref('agg_daily_zone') }}) - (select sum(total_amount) from {{ ref('fct_trips') }})) > 1
