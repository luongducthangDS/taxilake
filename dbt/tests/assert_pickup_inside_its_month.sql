-- Every trip sits in the month partition it was loaded into (OUT_OF_PERIOD rows must be quarantined).
select trip_key from {{ ref('fct_trips') }}
where date_trunc('month', pickup_at) <> pickup_month
