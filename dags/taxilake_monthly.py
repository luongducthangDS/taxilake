"""Monthly NYC yellow-taxi load: landing -> bronze -> silver -> quality gate -> dbt gold.

One DAG run = one month (the run's data interval). Re-running a month is safe: every step overwrites that
month's partition, and fct_trips uses delete+insert on pickup_month.
"""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT = Path(os.getenv("TAXILAKE_HOME", Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(PROJECT))

MAX_QUARANTINE_RATE = 0.05  # stop before gold if more than 5 % of a month is rejected


def _month(context) -> str:
    return context["data_interval_start"].strftime("%Y-%m")


def bronze(**context):
    from taxilake.bronze import build_bronze
    return build_bronze(_month(context))


def silver(**context):
    from taxilake.silver import build_silver
    return build_silver(_month(context))


def quality_gate(**context):
    metrics = context["ti"].xcom_pull(task_ids="silver")
    rate = metrics["rows_quarantined"] / metrics["rows_in"]
    print(f"{_month(context)}: quarantine rate {rate:.2%} ({metrics['details']})")
    if rate > MAX_QUARANTINE_RATE:
        # fail without retries: retrying will not fix bad source data, a human has to look
        raise AirflowFailException(f"quarantine rate {rate:.2%} > {MAX_QUARANTINE_RATE:.0%}")


with DAG(
    dag_id="taxilake_monthly",
    description="NYC TLC yellow trips: bronze/silver lake + dbt star schema",
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2021, 7, 31),
    schedule="@monthly",
    catchup=True,            # backfill = just let Airflow run the missing months
    max_active_runs=1,       # DuckDB allows one writer at a time
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["taxilake"],
) as dag:
    t_bronze = PythonOperator(task_id="bronze", python_callable=bronze)
    t_silver = PythonOperator(task_id="silver", python_callable=silver)
    t_gate = PythonOperator(task_id="quality_gate", python_callable=quality_gate, retries=0)
    t_dbt = BashOperator(
        task_id="dbt_build",
        cwd=str(PROJECT / "dbt"),
        bash_command=(
            "dbt build --select +fct_trips+ "
            "--vars '{months: [\"{{ data_interval_start.strftime('%Y-%m-01') }}\"]}'"
        ),
        env={"DBT_PROFILES_DIR": str(PROJECT / "dbt"),
             "TAXILAKE_DATA_DIR": os.getenv("TAXILAKE_DATA_DIR", str(PROJECT / "data"))},
        append_env=True,
    )

    t_bronze >> t_silver >> t_gate >> t_dbt
