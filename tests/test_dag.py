"""DAG integrity: imports cleanly, tasks in the right order, safe settings for DuckDB."""
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("taxilake_monthly")


def test_task_order(dag):
    order = [t.task_id for t in dag.topological_sort()]
    assert order == ["bronze", "silver", "quality_gate", "dbt_build"]


def test_single_writer_and_backfill(dag):
    assert dag.max_active_runs == 1   # DuckDB: one writer
    assert dag.catchup is True


def test_quality_gate_does_not_retry(dag):
    assert dag.get_task("quality_gate").retries == 0


def test_quality_gate_blocks_bad_month():
    from airflow.exceptions import AirflowFailException
    import importlib.util
    spec = importlib.util.spec_from_file_location("dagmod", DAGS / "taxilake_monthly.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    class TI:
        def xcom_pull(self, task_ids):
            return {"rows_in": 100, "rows_quarantined": 6, "details": "{}"}

    import pendulum
    ctx = {"ti": TI(), "data_interval_start": pendulum.datetime(2021, 6, 1)}
    with pytest.raises(AirflowFailException):
        mod.quality_gate(**ctx)
