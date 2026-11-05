"""Task final: genera el reporte (salida del DAG)."""
import json
from datetime import datetime
from pathlib import Path

from rg_config import REPORTS


def generar_reporte(datos, reports=REPORTS):
    """datos = {'fuentes','raw','processed','kpis','curated'} (llega por XCom). Devuelve la ruta."""
    reports = Path(reports)
    reports.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now()
    k = datos["kpis"]

    lineas = [
        "# Reporte de calidad y KPIs: flota RapidGo (Variante 8)",
        f"Generado: {ahora:%Y-%m-%d %H:%M:%S}",
        "",
        "## Calidad de datos",
        f"- Filas RAW: {datos['raw']['filas_raw']}",
        f"- Filas PROCESSED: {datos['processed']['filas_processed']}",
        f"- Filas CURATED: {datos['curated']['filas_curated']}",
        "",
        "## KPIs",
        f"- Vehiculos monitoreados: {k['vehiculos_total']}",
        f"- Vehiculos en riesgo: {k['vehiculos_en_riesgo']} ({k['pct_en_riesgo']}%)",
        f"- Alertas A1 (mantenimiento vencido): {k['alertas_a1']}",
        f"- Alertas A2 (temperatura critica): {k['alertas_a2']}",
        "- Fallas por tipo:",
    ]
    if k["fallas_por_tipo"]:
        lineas += [f"  - {codigo}: {n}" for codigo, n in sorted(k["fallas_por_tipo"].items())]
    else:
        lineas.append("  - (sin fallas)")

    ruta = reports / f"reporte_{ahora:%Y%m%d_%H%M%S}.md"
    ruta.write_text("\n".join(lineas) + "\n", encoding="utf-8")
    (reports / "ultimo_reporte.json").write_text(
        json.dumps(datos, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    return str(ruta)