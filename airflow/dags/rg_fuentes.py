"""Task 1: verificar que las fuentes existen (Kafka y capas del Data Lake)."""
import socket
from pathlib import Path

from rg_config import KAFKA_HOST, KAFKA_PORT, LAKE


def contar_parquet(ruta):
    """Cuenta archivos .parquet bajo una ruta (0 si la ruta no existe)."""
    ruta = Path(ruta)
    return len(list(ruta.rglob("*.parquet"))) if ruta.exists() else 0


def check_fuentes(lake=LAKE, host=KAFKA_HOST, port=KAFKA_PORT):
    """Falla con ConnectionError si Kafka no responde. Devuelve un resumen (va por XCom)."""
    try:
        with socket.create_connection((host, port), timeout=3):
            pass
    except OSError as e:
        raise ConnectionError(f"Kafka no disponible en {host}:{port} ({e})") from e

    lake = Path(lake)
    return {
        "kafka": f"{host}:{port}",
        "archivos_raw": contar_parquet(lake / "raw" / "telemetry"),
        "archivos_processed": contar_parquet(lake / "processed" / "telemetry"),
    }