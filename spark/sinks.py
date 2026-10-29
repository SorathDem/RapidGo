
from pathlib import Path

from spark import config as C


def escribir_parquet(df, nombre, ruta, particion=None):
    checkpoint = str(Path(C.CHECKPOINTS) / nombre)

    escritura = (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", str(Path(C.LAKE) / ruta))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="10 seconds")
    )

    if particion:
        escritura = escritura.partitionBy(particion)

    return escritura.start()
