-- Clean trips produced by the Python silver step (one Parquet file per month).
-- hive_partitioning exposes year=/month= folders as columns, so filters on them skip whole files.
select * rename (year as lake_year, month as lake_month)
from read_parquet('{{ var("lake_path") }}/silver/yellow_trips/*/*/data.parquet', hive_partitioning = true)
