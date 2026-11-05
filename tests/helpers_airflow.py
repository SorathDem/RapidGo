"""Utilidades de prueba para el DAG (P1 las incluye en el PR base)."""
import pandas as pd


def fila(vehicle_id="VEH-001", km_servicio=3000.0, temp_critica=False, fault_code=None, **extra):
    base = {
        "event_id": "b7c1f1e2-5a3d-4c0a-9d1e-2f6a8e3c4b10",
        "event_ts": pd.Timestamp("2026-11-03T10:00:00Z"),
        "vehicle_id": vehicle_id,
        "odometer_km": 15000.0,
        "km_since_last_service": km_servicio,
        "engine_temp_c": 110.0 if temp_critica else 90.0,
        "fault_code": fault_code,
        "tiene_falla": fault_code is not None,
        "temp_critica": temp_critica,
    }
    base.update(extra)
    return base


def escribir_processed(lake, filas):
    ruta = lake / "processed" / "telemetry"
    ruta.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(filas).to_parquet(ruta / "part-0000.snappy.parquet", index=False)
    return lake


def escribir_raw(lake, n=3):
    ruta = lake / "raw" / "telemetry"
    ruta.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"kafka_key": [f"VEH-00{i}" for i in range(n)]}).to_parquet(
        ruta / "part-0000.snappy.parquet", index=False
    )
    return lake