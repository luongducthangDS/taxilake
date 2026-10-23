"""Landing -> bronze: download the monthly file and store it as typed Parquet with lineage columns.

Bronze keeps every row, including bad ones. Cleaning happens in silver, so bronze can always be
re-processed when a rule changes.
"""
import time
import urllib.request

import duckdb

from taxilake.config import BRONZE, LANDING, RAW_SCHEMA, SOURCE_URL, partition
from taxilake.io import log_run, write_partition_atomically


def landing_file(month: str):
    return LANDING / f"yellow_tripdata_{month}.csv.gz"


def download(month: str, retries: int = 3) -> None:
    """Download once; skip when the file is already there (re-runs cost nothing)."""
    target = landing_file(month)
    if target.exists() and target.stat().st_size > 0:
        return
    LANDING.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".part")
    for attempt in range(1, retries + 1):
        try:
            urllib.request.urlretrieve(SOURCE_URL.format(month=month), tmp)
            tmp.rename(target)
            return
        except OSError:
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)  # exponential backoff


def build_bronze(month: str) -> dict:
    started = time.perf_counter()
    download(month)
    src = landing_file(month)
    columns = ", ".join(f"'{k}': '{v}'" for k, v in RAW_SCHEMA.items())
    query = f"""
        SELECT *,
               '{src.name}'            AS _source_file,
               row_number() OVER ()    AS _source_row,      -- stable lineage back to the raw file
               current_timestamp       AS _ingested_at
        FROM read_csv('{src}', header = true, columns = {{{columns}}},
                      timestampformat = '%Y-%m-%d %H:%M:%S')
    """
    with duckdb.connect() as con:
        con.execute("SET enable_progress_bar = false")
        write_partition_atomically(con, query, partition(BRONZE, month))
        rows = con.execute(f"SELECT count(*) FROM '{partition(BRONZE, month)}/data.parquet'").fetchone()[0]
    metrics = {"rows_in": rows, "rows_out": rows, "rows_quarantined": 0,
               "seconds": round(time.perf_counter() - started, 2)}
    log_run("bronze", month, metrics)
    return metrics
