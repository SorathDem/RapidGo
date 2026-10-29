from pyspark.sql import SparkSession

from spark import config as C
from spark.sinks import escribir_parquet
from spark.transformations import parsear, condicion_valido, transformar
from spark.aggregations import iniciar_curated


def crear_spark():
    return (
        SparkSession.builder.appName("rapidgo-flota-streaming")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )


def leer_kafka(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", C.KAFKA_BOOTSTRAP)
        .option("subscribe", C.TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def iniciar_queries(spark):
    """Arranca RAW, PROCESSED y RECHAZADOS. Devuelve el DataFrame procesado."""
    parsed = parsear(leer_kafka(spark))
    valido = condicion_valido()

    raw = parsed.select("kafka_key", "raw_value", "kafka_partition", "kafka_offset", "kafka_timestamp")
    escribir_parquet(raw, "raw", "raw/telemetry")

    procesado = transformar(parsed.filter(valido))
    escribir_parquet(procesado, "processed", "processed/telemetry", "event_date")

    rechazados = parsed.filter(~valido).select("kafka_key", "raw_value", "kafka_partition", "kafka_offset")
    escribir_parquet(rechazados, "rejected", "processed/rejected")

    return procesado


def main():
    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")
    procesado = iniciar_queries(spark)
    iniciar_curated(procesado)
    spark.streams.awaitAnyTermination()
    


if __name__ == "__main__":
    main()