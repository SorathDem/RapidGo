"""P1: pruebas de estructura del DAG. Se saltan solas si Airflow no esta instalado (p. ej. Windows local)."""
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")  # ojo: "airflow.models", no "airflow" (la carpeta airflow/ del repo engaña)
from airflow.models import DagBag  # noqa: E402

RAIZ = Path(__file__).resolve().parent.parent
DAG_ID = "rapidgo_flota_calidad"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(RAIZ / "airflow" / "dags"), include_examples=False)


def test_dag_se_importa_sin_errores(dagbag):
    assert not dagbag.import_errors, dagbag.import_errors
    assert DAG_ID in dagbag.dags


def test_dag_tiene_al_menos_5_tasks(dagbag):
    assert len(dagbag.dags[DAG_ID].tasks) >= 5


def test_todas_las_tasks_tienen_retries(dagbag):
    for t in dagbag.dags[DAG_ID].tasks:
        assert t.retries >= 1, f"{t.task_id} sin retries"