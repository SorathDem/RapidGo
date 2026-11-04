"""Task 'agregar': KPIs de flota a partir de PROCESSED (batch)."""
from pathlib import Path

import pandas as pd

from rg_config import LAKE, MIN_LECTURAS_CRITICAS, UMBRAL_KM_SERVICIO


def agregar_kpis(lake=LAKE):
    """Escribe curated/kpis_flota/kpis_flota.parquet y devuelve un resumen serializable (XCom)."""
    df = pd.read_parquet(Path(lake) / "processed" / "telemetry")

    por_veh = (
        df.groupby("vehicle_id")
        .agg(
            odometro_max=("odometer_km", "max"),
            km_desde_servicio_max=("km_since_last_service", "max"),
            lecturas_criticas=("temp_critica", "sum"),
            n_fallas=("tiene_falla", "sum"),
        )
        .reset_index()
    )
    por_veh["alerta_a1"] = por_veh["km_desde_servicio_max"] > UMBRAL_KM_SERVICIO
    por_veh["alerta_a2"] = por_veh["lecturas_criticas"] >= MIN_LECTURAS_CRITICAS
    por_veh["en_riesgo"] = por_veh["alerta_a1"] | por_veh["alerta_a2"]

    salida = Path(lake) / "curated" / "kpis_flota"
    salida.mkdir(parents=True, exist_ok=True)
    por_veh.to_parquet(salida / "kpis_flota.parquet", index=False)

    total = int(len(por_veh))
    en_riesgo = int(por_veh["en_riesgo"].sum())
    fallas = df[df["fault_code"].notna()].groupby("fault_code").size()
    return {
        "vehiculos_total": total,
        "vehiculos_en_riesgo": en_riesgo,
        "pct_en_riesgo": round(100 * en_riesgo / total, 1) if total else 0.0,
        "alertas_a1": int(por_veh["alerta_a1"].sum()),
        "alertas_a2": int(por_veh["alerta_a2"].sum()),
        "fallas_por_tipo": {str(k): int(v) for k, v in fallas.items()},
    }
