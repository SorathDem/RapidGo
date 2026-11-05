"""P4: el reporte final refleja los KPIs recibidos por XCom."""
from rg_reporte import generar_reporte


def test_reporte_incluye_kpis(tmp_path):
    datos = {
        "fuentes": {"kafka": "kafka:9092", "archivos_raw": 2, "archivos_processed": 2},
        "raw": {"filas_raw": 100},
        "processed": {"filas_processed": 95},
        "kpis": {
            "vehiculos_total": 4, "vehiculos_en_riesgo": 2, "pct_en_riesgo": 50.0,
            "alertas_a1": 1, "alertas_a2": 1, "fallas_por_tipo": {"P0217": 3},
        },
        "curated": {"filas_curated": 4},
    }
    ruta = generar_reporte(datos, reports=tmp_path)
    texto = open(ruta, encoding="utf-8").read()
    assert "50.0%" in texto and "P0217: 3" in texto and "Filas RAW: 100" in texto
    assert (tmp_path / "ultimo_reporte.json").exists()