"""DAG de calidad y KPIs de la flota (Variante 8). Semana 3.

check_fuentes -> validar_raw -> validar_processed -> agregar_kpis -> validar_curated -> generar_reporte

Los datos pasan entre tasks por XCom (TaskFlow API: el return de una task es el argumento de la siguiente).
Los modulos se importan dentro de cada task para que el DAG cargue rapido.
"""
import logging
from datetime import datetime

from airflow.decorators import dag, task

from rg_config import RETRIES, RETRY_DELAY

log = logging.getLogger("airflow.task")

DEFAULT_ARGS = {
    "owner": "equipo6-flota",
    "retries": RETRIES,
    "retry_delay": RETRY_DELAY,
}


@dag(
    dag_id="rapidgo_flota_calidad",
    description="Calidad de datos y KPIs de la flota RapidGo",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["rapidgo", "flota", "semana3"],
)
def rapidgo_flota_calidad():
    @task
    def check_fuentes():
        from rg_fuentes import check_fuentes as _check
        return _check()

    @task
    def validar_raw(fuentes: dict):
        from rg_calidad import validar_raw as _validar
        log.info("Archivos RAW detectados en check_fuentes: %s", fuentes["archivos_raw"])
        return _validar()

    @task
    def validar_processed(raw: dict):
        from rg_calidad import validar_processed as _validar
        log.info("Filas RAW recibidas por XCom: %s", raw["filas_raw"])
        return _validar()

    @task
    def agregar_kpis(processed: dict):
        from rg_kpis import agregar_kpis as _agregar
        log.info("Filas PROCESSED recibidas por XCom: %s", processed["filas_processed"])
        return _agregar()

    @task
    def validar_curated(kpis: dict):
        from rg_calidad import validar_curated as _validar
        return _validar(kpis)

    @task
    def generar_reporte(fuentes: dict, raw: dict, processed: dict, kpis: dict, curated: dict):
        from rg_reporte import generar_reporte as _reporte
        ruta = _reporte(
            {"fuentes": fuentes, "raw": raw, "processed": processed, "kpis": kpis, "curated": curated}
        )
        log.info("Reporte generado: %s", ruta)
        return ruta

    f = check_fuentes()
    r = validar_raw(f)
    p = validar_processed(r)
    k = agregar_kpis(p)
    c = validar_curated(k)
    generar_reporte(f, r, p, k, c)


rapidgo_flota_calidad()