"""Tasks de calidad: validar RAW, PROCESSED y CURATED."""
from pathlib import Path

import pandas as pd

from rg_config import LAKE
from rg_fuentes import contar_parquet


class CalidadError(Exception):
    """Fallo de calidad de datos. Hace fallar la task en Airflow."""



COLUMNAS_PROCESSED = {
    "kafka_key",
    "kafka_partition",
    "kafka_offset",
    "event_id",
    "event_time",
    "vehicle_id",
    "vehicle_type",
    "odometer_km",
    "km_since_last_service",
    "engine_temp_c",
    "fault_code",
    "usage_hours_today",
    "event_ts",
    "tiene_falla",
    "mant_vencido",
    "temp_critica",
    "banda_temp",
    "event_date",
}



def validar_raw(lake=LAKE):
    ruta = Path(lake) / "raw" / "telemetry"
    if contar_parquet(ruta) == 0:
        raise CalidadError(f"RAW vacio: no hay archivos Parquet en {ruta}")
    filas = len(pd.read_parquet(ruta, columns=["kafka_key"]))
    if filas == 0:
        raise CalidadError(f"RAW sin filas en {ruta}")
    return {"filas_raw": filas}


def validar_processed(lake=LAKE):
    ruta = Path(lake) / "processed" / "telemetry"
    if contar_parquet(ruta) == 0:
        raise CalidadError(f"PROCESSED vacio: no hay archivos Parquet en {ruta}")
    df = pd.read_parquet(ruta)
    faltantes = COLUMNAS_PROCESSED - set(df.columns)
    if faltantes:
        raise CalidadError(f"Esquema invalido en PROCESSED, faltan columnas: {sorted(faltantes)}")
    nulos = int(df["vehicle_id"].isna().sum())
    if nulos:
        raise CalidadError(f"PROCESSED tiene {nulos} filas con vehicle_id nulo")
    return {"filas_processed": int(len(df))}


def validar_curated(resumen_kpis, lake=LAKE):
    ruta = Path(lake) / "curated" / "kpis_flota" / "kpis_flota.parquet"
    if not ruta.exists():
        raise CalidadError(f"CURATED no existe: {ruta}")
    df = pd.read_parquet(ruta)
    if len(df) != resumen_kpis["vehiculos_total"]:
        raise CalidadError(
            f"CURATED inconsistente: {len(df)} filas vs {resumen_kpis['vehiculos_total']} vehiculos"
        )
    if not 0 <= resumen_kpis["pct_en_riesgo"] <= 100:
        raise CalidadError(f"pct_en_riesgo fuera de rango: {resumen_kpis['pct_en_riesgo']}")
    return {"filas_curated": int(len(df))}
