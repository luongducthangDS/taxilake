"""Unit tests for bronze/silver on tiny hand-made files: one row per rule, so every rule is proven."""
import gzip
import importlib
from pathlib import Path

import duckdb
import pytest

HEADER = ("VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,"
          "store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,"
          "tolls_amount,improvement_surcharge,total_amount,congestion_surcharge")


def row(pickup="2021-07-10 10:00:00", dropoff="2021-07-10 10:20:00", dist="3.5", pu="142", do="236",
        fare="15", total="20.3", vendor="1"):
    return f"{vendor},{pickup},{dropoff},1,{dist},1,N,{pu},{do},1,{fare},0.5,0.5,1,0,0.3,{total},2.5"


CASES = {  # expected reason -> raw line
    None: row(),
    "MISSING_TIMESTAMP": row(dropoff=""),
    "OUT_OF_PERIOD": row(pickup="2008-12-31 23:05:14", dropoff="2008-12-31 23:30:00"),
    "NON_POSITIVE_DURATION": row(dropoff="2021-07-10 09:59:00"),
    "EXCESSIVE_DURATION": row(dropoff="2021-07-11 02:00:00"),
    "NON_POSITIVE_DISTANCE": row(dist="0"),
    "EXCESSIVE_DISTANCE": row(dist="250"),
    "NEGATIVE_AMOUNT": row(fare="-15", total="-20.3"),
    "UNKNOWN_ZONE": row(pu="300"),
}


@pytest.fixture
def lake(tmp_path, monkeypatch):
    """Point the pipeline at an empty temp data dir and reload modules that read the config."""
    monkeypatch.setenv("TAXILAKE_DATA_DIR", str(tmp_path))
    import taxilake.bronze
    import taxilake.config
    import taxilake.io
    import taxilake.silver
    for mod in (taxilake.config, taxilake.io, taxilake.bronze, taxilake.silver):
        importlib.reload(mod)
    return tmp_path


def write_landing(lake: Path, month: str, lines: list[str]) -> None:
    landing = lake / "landing"
    landing.mkdir(parents=True, exist_ok=True)
    with gzip.open(landing / f"yellow_tripdata_{month}.csv.gz", "wt") as f:
        f.write(HEADER + "\n" + "\n".join(lines) + "\n")


def read(path: Path, sql: str = "SELECT * FROM t"):
    con = duckdb.connect()
    con.execute(f"CREATE VIEW t AS SELECT * FROM '{path}/data.parquet'")
    cur = con.execute(sql)
    return [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]


def run_month(lake, month="2021-07"):
    from taxilake.bronze import build_bronze
    from taxilake.silver import build_silver
    return build_bronze(month), build_silver(month)


def test_each_rule_catches_its_row_and_clean_row_passes(lake):
    write_landing(lake, "2021-07", list(CASES.values()))
    _, s = run_month(lake)
    from taxilake.config import QUARANTINE, SILVER, partition
    reasons = sorted(r["reject_reason"] for r in read(partition(QUARANTINE, "2021-07")))
    assert reasons == sorted(k for k in CASES if k)
    silver = read(partition(SILVER, "2021-07"))
    assert len(silver) == 1
    assert silver[0]["trip_minutes"] == 20.0
    assert silver[0]["pickup_zone_id"] == 142


def test_every_bronze_row_lands_in_exactly_one_place(lake):
    write_landing(lake, "2021-07", list(CASES.values()) * 3)
    b, s = run_month(lake)
    assert b["rows_in"] == s["rows_out"] + s["rows_quarantined"] == 27


def test_exact_duplicates_keep_first_occurrence(lake):
    write_landing(lake, "2021-07", [row(), row(), row(fare="16")])
    _, s = run_month(lake)
    assert s["rows_out"] == 2
    assert s["rows_quarantined"] == 1
    from taxilake.config import QUARANTINE, partition
    dup = read(partition(QUARANTINE, "2021-07"))[0]
    assert dup["reject_reason"] == "DUPLICATE"
    assert dup["_source_row"] == 2  # the second copy is the one rejected


def test_rerun_is_idempotent(lake):
    write_landing(lake, "2021-07", list(CASES.values()))
    first = run_month(lake)[1]
    second = run_month(lake)[1]
    assert (first["rows_out"], first["rows_quarantined"]) == (second["rows_out"], second["rows_quarantined"])
    from taxilake.config import SILVER, partition
    files = list(partition(SILVER, "2021-07").parent.rglob("*.parquet"))
    assert len(files) == 1                                   # replaced, not appended
    assert not list(partition(SILVER, "2021-07").parent.glob(".tmp-*"))  # no leftovers


def test_trip_key_is_stable_across_reruns(lake):
    write_landing(lake, "2021-07", [row(), row(fare="16")])
    from taxilake.config import SILVER, partition
    run_month(lake)
    keys1 = {r["trip_key"] for r in read(partition(SILVER, "2021-07"))}
    run_month(lake)
    keys2 = {r["trip_key"] for r in read(partition(SILVER, "2021-07"))}
    assert keys1 == keys2 and len(keys1) == 2


def test_failed_write_keeps_previous_partition(lake):
    write_landing(lake, "2021-07", [row()])
    run_month(lake)
    from taxilake.config import SILVER, partition
    from taxilake.io import write_partition_atomically
    with duckdb.connect() as con, pytest.raises(duckdb.Error):
        write_partition_atomically(con, "SELECT * FROM no_such_table", partition(SILVER, "2021-07"))
    assert len(read(partition(SILVER, "2021-07"))) == 1  # old data still readable


def test_partial_records_are_kept_and_flagged(lake):
    write_landing(lake, "2021-07", [row(vendor="")])
    _, s = run_month(lake)
    from taxilake.config import SILVER, partition
    assert s["rows_out"] == 1
    assert read(partition(SILVER, "2021-07"))[0]["is_partial_record"] is True


def test_run_log_records_every_step(lake):
    write_landing(lake, "2021-07", [row()])
    run_month(lake)
    from taxilake.config import WAREHOUSE
    with duckdb.connect(str(WAREHOUSE)) as con:
        steps = con.execute("SELECT step FROM meta.load_log ORDER BY run_at").fetchall()
    assert [s[0] for s in steps] == ["bronze", "silver"]


@pytest.mark.parametrize("spec, expected", [
    ("2021-07", ["2021-07"]),
    ("2020-11:2021-02", ["2020-11", "2020-12", "2021-01", "2021-02"]),
])
def test_month_range(spec, expected):
    from taxilake.cli import month_range
    assert month_range(spec) == expected
