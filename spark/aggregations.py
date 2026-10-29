from pyspark.sql import functions as F

from spark import config as C
from spark.sinks import escribir_parquet


def kpis_por_vehiculo(procesado):
    return (
        procesado.withWatermark("event_ts", C.WATERMARK)
        .groupBy(F.window("event_ts", C.WINDOW), "vehicle_id")
        .agg(
            F.count("*").alias("n_eventos"),
            F.max("odometer_km").alias("odometro_max"),
            (F.max("odometer_km") - F.min("odometer_km")).alias("km_recorridos"),
            F.max("km_since_last_service").alias("km_desde_servicio_max"),
            F.sum(F.col("temp_critica").cast("int")).alias("lecturas_criticas"),
            F.sum(F.col("tiene_falla").cast("int")).alias("n_fallas"),
        )
        .withColumn("alerta_a1", F.col("km_desde_servicio_max") > C.UMBRAL_KM_SERVICIO)
        .withColumn("alerta_a2", F.col("lecturas_criticas") >= C.MIN_LECTURAS_CRITICAS)
        .withColumn("en_riesgo", F.col("alerta_a1") | F.col("alerta_a2"))
        .select(
            F.col("window.start").alias("ventana_inicio"),
            F.col("window.end").alias("ventana_fin"),
            "vehicle_id", "n_eventos", "odometro_max", "km_recorridos",
            "km_desde_servicio_max", "lecturas_criticas", "n_fallas",
            "alerta_a1", "alerta_a2", "en_riesgo",
        )
    )


def fallas_por_tipo(procesado):
    return (
        procesado.filter(F.col("fault_code").isNotNull())
        .withWatermark("event_ts", C.WATERMARK)
        .groupBy(F.window("event_ts", C.WINDOW), "fault_code")
        .agg(F.count("*").alias("n_fallas"))
        .select(
            F.col("window.start").alias("ventana_inicio"),
            F.col("window.end").alias("ventana_fin"),
            "fault_code", "n_fallas",
        )
    )


def iniciar_curated(procesado):
    kpis = kpis_por_vehiculo(procesado)
    escribir_parquet(kpis, "curated_kpis_vehiculo", "curated/kpis_vehiculo")
    escribir_parquet(fallas_por_tipo(procesado), "curated_fallas_tipo", "curated/fallas_por_tipo")
    escribir_parquet(kpis.filter(F.col("alerta_a1") | F.col("alerta_a2")),
                     "curated_alertas", "curated/alertas")