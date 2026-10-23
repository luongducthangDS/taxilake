"""Command line entry point.

    python -m taxilake.cli run --months 2021-01:2021-07     # bronze + silver for a range
    python -m taxilake.cli bronze --months 2021-07
    python -m taxilake.cli silver --months 2021-07
"""
import argparse
import time

from taxilake.bronze import build_bronze
from taxilake.silver import build_silver


def month_range(spec: str) -> list[str]:
    """'2021-01:2021-03' -> ['2021-01', '2021-02', '2021-03']; '2021-05' -> ['2021-05']."""
    start, _, end = spec.partition(":")
    end = end or start
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    months = []
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def main() -> None:
    parser = argparse.ArgumentParser(prog="taxilake")
    parser.add_argument("step", choices=["bronze", "silver", "run"])
    parser.add_argument("--months", required=True, help="YYYY-MM or YYYY-MM:YYYY-MM")
    args = parser.parse_args()

    for month in month_range(args.months):
        t = time.perf_counter()
        if args.step in ("bronze", "run"):
            b = build_bronze(month)
            print(f"{month} bronze  {b['rows_in']:>10,} rows")
        if args.step in ("silver", "run"):
            s = build_silver(month)
            pct = 100 * s["rows_quarantined"] / s["rows_in"]
            print(f"{month} silver  {s['rows_out']:>10,} rows  quarantined {s['rows_quarantined']:>8,} ({pct:.2f}%)")
        print(f"{month} done in {time.perf_counter() - t:.1f}s")


if __name__ == "__main__":
    main()
