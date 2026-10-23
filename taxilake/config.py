"""Paths and constants. Everything is relative to TAXILAKE_DATA_DIR so the same code runs locally and in Airflow."""
import os
from pathlib import Path

DATA_DIR = Path(os.getenv("TAXILAKE_DATA_DIR", Path(__file__).resolve().parents[1] / "data"))
LANDING = DATA_DIR / "landing"          # raw files exactly as downloaded
BRONZE = DATA_DIR / "lake" / "bronze" / "yellow_trips"
SILVER = DATA_DIR / "lake" / "silver" / "yellow_trips"
QUARANTINE = DATA_DIR / "lake" / "quarantine" / "yellow_trips"
WAREHOUSE = DATA_DIR / "warehouse.duckdb"  # run log + dbt gold models

SOURCE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{month}.csv.gz"

# Raw schema is declared, never inferred: inference on a 6M-row CSV is slow and silently changes between months.
RAW_SCHEMA = {
    "VendorID": "INTEGER",
    "tpep_pickup_datetime": "TIMESTAMP",
    "tpep_dropoff_datetime": "TIMESTAMP",
    "passenger_count": "INTEGER",
    "trip_distance": "DOUBLE",
    "RatecodeID": "INTEGER",
    "store_and_fwd_flag": "VARCHAR",
    "PULocationID": "INTEGER",
    "DOLocationID": "INTEGER",
    "payment_type": "INTEGER",
    "fare_amount": "DOUBLE",
    "extra": "DOUBLE",
    "mta_tax": "DOUBLE",
    "tip_amount": "DOUBLE",
    "tolls_amount": "DOUBLE",
    "improvement_surcharge": "DOUBLE",
    "total_amount": "DOUBLE",
    "congestion_surcharge": "DOUBLE",
}


def partition(base: Path, month: str) -> Path:
    year, mm = month.split("-")
    return base / f"year={year}" / f"month={mm}"
