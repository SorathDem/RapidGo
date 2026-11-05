"""Configuracion compartida del DAG de calidad.

Los umbrales deben coincidir con spark/config.py (un test lo verifica).
"""
import os
from datetime import timedelta
from pathlib import Path

LAKE = Path(os.getenv("LAKE_PATH", "datalake"))
REPORTS = Path(os.getenv("REPORTS_PATH", "reports"))
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "9092"))

UMBRAL_KM_SERVICIO = 5000.0
UMBRAL_TEMP_C = 105.0
MIN_LECTURAS_CRITICAS = 2

RETRIES = 2
RETRY_DELAY = timedelta(seconds=20)