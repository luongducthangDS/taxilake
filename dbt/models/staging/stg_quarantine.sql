-- Rejected rows with the rule that rejected them, for data-quality reporting.
select date_trunc('month', coalesce(tpep_pickup_datetime, tpep_dropoff_datetime)) as event_month,
       _source_file, reject_reason
from read_parquet('{{ var("lake_path") }}/quarantine/yellow_trips/*/*/data.parquet')
