"""P4: el orden del flujo es el disenado."""
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")
from airflow.models import DagBag  # noqa: E402

RAIZ = Path(__file__).resolve().parent.parent
DAG_ID = "rapidgo_flota_calidad"

FLUJO = [
    ("check_fuentes", "validar_raw"),
    ("validar_raw", "validar_processed"),
    ("validar_processed", "agregar_kpis"),
    ("agregar_kpis", "validar_curated"),
    ("validar_curated", "generar_reporte"),
]


def test_dependencias_del_flujo():
    dag = DagBag(dag_folder=str(RAIZ / "airflow" / "dags"), include_examples=False).dags[DAG_ID]
    for antes, despues in FLUJO:
        assert despues in dag.get_task(antes).downstream_task_ids, f"falta {antes} >> {despues}"