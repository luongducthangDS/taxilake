-- How many rows each rule rejected per source file: the data-quality scorecard.
select _source_file, reject_reason, count(*) as rows_rejected
from {{ ref('stg_quarantine') }}
group by all
