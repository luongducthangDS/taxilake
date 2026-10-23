"""Small I/O helpers: atomic partition writes and the run log."""
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from taxilake.config import WAREHOUSE


def write_partition_atomically(con: duckdb.DuckDBPyConnection, query: str, target_dir: Path) -> None:
    """Write `query` as one Parquet file into target_dir, replacing whatever was there.

    Written to a temp dir first and swapped in with renames, so a crash mid-write never leaves a
    half-written partition that downstream jobs would read. Re-running a month therefore overwrites
    instead of appending: the load is idempotent.
    """
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp = target_dir.parent / f".tmp-{target_dir.name}-{uuid.uuid4().hex[:8]}"
    tmp.mkdir()
    con.execute(f"COPY ({query}) TO '{tmp / 'data.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    old = None
    if target_dir.exists():
        old = target_dir.parent / f".old-{target_dir.name}-{uuid.uuid4().hex[:8]}"
        target_dir.rename(old)
    tmp.rename(target_dir)
    if old is not None:
        shutil.rmtree(old)


def log_run(step: str, month: str, metrics: dict) -> None:
    """Append one row per (step, month) run to meta.load_log in the warehouse."""
    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(WAREHOUSE)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS meta")
        con.execute("""
            CREATE TABLE IF NOT EXISTS meta.load_log (
                run_at TIMESTAMPTZ, step VARCHAR, month VARCHAR,
                rows_in BIGINT, rows_out BIGINT, rows_quarantined BIGINT,
                seconds DOUBLE, details JSON)""")
        con.execute(
            "INSERT INTO meta.load_log VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [datetime.now(timezone.utc), step, month, metrics.get("rows_in"), metrics.get("rows_out"),
             metrics.get("rows_quarantined"), metrics.get("seconds"), metrics.get("details", "{}")],
        )
