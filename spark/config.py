import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "rapidgo.fleet.telemetry"
LAKE = os.getenv("LAKE_PATH", "datalake")
CHECKPOINTS = os.getenv("CHECKPOINT_PATH", "checkpoints")
WINDOW = os.getenv("WINDOW_DURATION", "5 minutes")
WATERMARK = os.getenv("WATERMARK", "1 minute")

# Umbrales de alerta (ver docs/kpis-alertas.md)
UMBRAL_KM_SERVICIO = 5000.0
UMBRAL_TEMP_C = 105.0
MIN_LECTURAS_CRITICAS = 2