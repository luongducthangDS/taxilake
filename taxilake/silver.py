"""Bronze -> silver + quarantine.

Each row gets the code of the FIRST rule it breaks. Rows that pass go to silver (clean, renamed, typed);
rows that fail go to quarantine with the reason, so nothing is dropped silently and every rejection can be
counted, explained and re-processed when a rule changes.
"""
import json
import time

import duckdb

from taxilake.config import BRONZE, QUARANTINE, SILVER, partition
from taxilake.io import log_run, write_partition_atomically

MAX_TRIP_HOURS = 12
MAX_TRIP_MILES = 200

# (code, condition that makes the row INVALID). Order matters: first match wins.
RULES: list[tuple[str, str]] = [
    ("MISSING_TIMESTAMP", "tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL"),
    ("OUT_OF_PERIOD", "date_trunc('month', tpep_pickup_datetime) <> DATE '{month}-01'"),
    ("NON_POSITIVE_DURATION", "tpep_dropoff_datetime <= tpep_pickup_datetime"),
    ("EXCESSIVE_DURATION", f"tpep_dropoff_datetime - tpep_pickup_datetime > INTERVAL {MAX_TRIP_HOURS} HOUR"),
    ("NON_POSITIVE_DISTANCE", "trip_distance IS NULL OR trip_distance <= 0"),
    ("EXCESSIVE_DISTANCE", f"trip_distance > {MAX_TRIP_MILES}"),
    ("NEGATIVE_AMOUNT", "fare_amount < 0 OR total_amount < 0"),
    ("UNKNOWN_ZONE", "PULocationID NOT BETWEEN 1 AND 265 OR DOLocationID NOT BETWEEN 1 AND 265"),
    ("DUPLICATE", "_dup_rank > 1"),
]

# Natural key used to detect exact duplicate trips (TLC has no trip id).
DEDUP_KEY = ("VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, "
             "passenger_count, trip_distance, fare_amount, total_amount, payment_type")


def _classified(month: str) -> str:
    case = "\n".join(f"WHEN {cond.format(month=month)} THEN '{code}'" for code, cond in RULES)
    return f"""
        WITH ranked AS (
            SELECT *, row_number() OVER (PARTITION BY {DEDUP_KEY} ORDER BY _source_row) AS _dup_rank
            FROM '{partition(BRONZE, month)}/data.parquet'
        )
        SELECT *, CASE {case} ELSE NULL END AS reject_reason FROM ranked
    """


def _silver_select(month: str) -> str:
    return f"""
        SELECT
            md5(_source_file || ':' || _source_row)                    AS trip_key,
            DATE '{month}-01'                                          AS pickup_month,
            VendorID                                                   AS vendor_id,
            tpep_pickup_datetime                                       AS pickup_at,
            tpep_dropoff_datetime                                      AS dropoff_at,
            round(epoch(tpep_dropoff_datetime - tpep_pickup_datetime) / 60.0, 2) AS trip_minutes,
            passenger_count,
            trip_distance                                              AS trip_miles,
            RatecodeID                                                 AS rate_code_id,
            store_and_fwd_flag = 'Y'                                   AS store_and_forward,
            PULocationID                                               AS pickup_zone_id,
            DOLocationID                                               AS dropoff_zone_id,
            payment_type                                               AS payment_type_id,
            fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
            coalesce(congestion_surcharge, 0)                          AS congestion_surcharge,
            total_amount,
            VendorID IS NULL                                           AS is_partial_record,
            _source_file, _source_row, _ingested_at
        FROM ({_classified(month)})
        WHERE reject_reason IS NULL
    """


def build_silver(month: str) -> dict:
    started = time.perf_counter()
    with duckdb.connect() as con:
        con.execute("SET enable_progress_bar = false")
        write_partition_atomically(con, _silver_select(month), partition(SILVER, month))
        write_partition_atomically(
            con,
            f"SELECT * EXCLUDE (_dup_rank) FROM ({_classified(month)}) WHERE reject_reason IS NOT NULL",
            partition(QUARANTINE, month),
        )
        rows_in = con.execute(f"SELECT count(*) FROM '{partition(BRONZE, month)}/data.parquet'").fetchone()[0]
        rows_out = con.execute(f"SELECT count(*) FROM '{partition(SILVER, month)}/data.parquet'").fetchone()[0]
        reasons = dict(con.execute(
            f"SELECT reject_reason, count(*) FROM '{partition(QUARANTINE, month)}/data.parquet' GROUP BY 1"
        ).fetchall())
    quarantined = sum(reasons.values())
    # reconciliation: every bronze row must land in exactly one place
    if rows_out + quarantined != rows_in:
        raise RuntimeError(f"{month}: {rows_in} in != {rows_out} silver + {quarantined} quarantine")
    metrics = {"rows_in": rows_in, "rows_out": rows_out, "rows_quarantined": quarantined,
               "seconds": round(time.perf_counter() - started, 2), "details": json.dumps(reasons)}
    log_run("silver", month, metrics)
    return metrics
