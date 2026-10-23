{#
  Grain: one row per valid yellow-taxi trip.
  Incremental by month with delete+insert: re-loading a month replaces it, so re-runs never duplicate.
    dbt build --vars '{months: ["2021-07-01"]}'   -> reprocess exactly that month
    dbt build                                      -> append months newer than the table's max month
    dbt build --full-refresh                       -> rebuild everything
#}
{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='pickup_month') }}

select
    trip_key,
    pickup_month,
    cast(pickup_at as date)                 as pickup_date,
    pickup_at,
    dropoff_at,
    pickup_zone_id,
    dropoff_zone_id,
    coalesce(vendor_id, -1)                 as vendor_id,
    coalesce(payment_type_id, -1)           as payment_type_id,
    coalesce(rate_code_id, -1)              as rate_code_id,
    passenger_count,
    trip_minutes,
    trip_miles,
    fare_amount,
    tip_amount,
    tolls_amount,
    extra + mta_tax + improvement_surcharge + congestion_surcharge as surcharges_amount,
    total_amount,
    is_partial_record
from {{ ref('stg_yellow_trips') }}

{% if is_incremental() %}
  {#- Decide which months to load, then filter on the folder columns so DuckDB opens only those files. -#}
  {% if var('months') | length > 0 %}
    {% set months = var('months') %}
  {% else %}
    {% set new_months = run_query(
        "select distinct strftime(pickup_month, '%Y-%m-%d') from " ~ ref('stg_yellow_trips') ~
        " where pickup_month > (select coalesce(max(pickup_month), date '1900-01-01') from " ~ this ~ ")") %}
    {% set months = new_months.columns[0].values() if execute else [] %}
  {% endif %}
  {% if months | length == 0 %}
where false
  {% else %}
where (lake_year, lake_month) in (
    {%- for m in months %}({{ m[:4] }}, '{{ m[5:7] }}'){{ "," if not loop.last }}{% endfor -%}
)
  {% endif %}
{% endif %}
