"""P2: logica de agregacion y consistencia de umbrales."""
import json

import pandas as pd
import pytest

import airflow.dags.rg_config as C
from airflow.dags.rg_kpis import agregar_kpis
from tests.helpers_airflow import escribir_processed, fila


def test_agregar_kpis_calcula_vehiculos_en_riesgo(tmp_path):
    escribir_processed(tmp_path, [
        fila("VEH-001", km_servicio=5100.0),                                   # A1
        fila("VEH-002", temp_critica=True), fila("VEH-002", temp_critica=True),  # A2 (2 lecturas)
        fila("VEH-003"),                                                        # normal
        fila("VEH-004", temp_critica=True),                                     # pico aislado: no alerta
        fila("VEH-004", fault_code="P0217"),
    ])
    r = agregar_kpis(tmp_path)

    assert r["vehiculos_total"] == 4
    assert r["vehiculos_en_riesgo"] == 2
    assert r["pct_en_riesgo"] == 50.0
    assert r["alertas_a1"] == 1 and r["alertas_a2"] == 1
    assert r["fallas_por_tipo"] == {"P0217": 1}
    json.dumps(r)  # debe ser serializable: va por XCom
    salida = pd.read_parquet(tmp_path / "curated" / "kpis_flota" / "kpis_flota.parquet")
    assert len(salida) == 4


def test_umbrales_del_dag_coinciden_con_spark():
    spark_cfg = pytest.importorskip("spark.config")
    assert C.UMBRAL_KM_SERVICIO == spark_cfg.UMBRAL_KM_SERVICIO
    assert C.UMBRAL_TEMP_C == spark_cfg.UMBRAL_TEMP_C
    assert C.MIN_LECTURAS_CRITICAS == spark_cfg.MIN_LECTURAS_CRITICAS