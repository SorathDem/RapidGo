# ADR-003 - Estrategia de orquestación y recuperación ante fallos

## 1. Contexto / problema

El pipeline de procesamiento de datos de RapidGo utiliza Apache Airflow para coordinar las diferentes tareas del proceso y Apache Kafka como servicio necesario para la disponibilidad de las fuentes de información.

Durante las pruebas se identificaron dos situaciones que podían afectar la ejecución del pipeline.

La primera ocurrió cuando el servicio Kafka no estaba disponible. Al intentar establecer la conexión, Airflow no pudo resolver el nombre del servicio `kafka` en el puerto 9092, lo que provocó que la tarea `check_fuentes` terminara en estado FAILED.

La segunda situación se presentó al retirar temporalmente la carpeta RAW de su ubicación habitual. Esto permitió comprobar si las validaciones de calidad detectaban la ausencia de archivos Parquet antes de continuar con el procesamiento.

## 2. Opciones consideradas

| Opción | Ventaja | Desventaja |
|---|---|---|
| Reintentos fijos (retries=2, 20 segundos) | Es una configuración sencilla y permite intentar nuevamente la conexión. | No garantiza la recuperación ante interrupciones prolongadas. |
| Reintentos con backoff exponencial | Permite ampliar progresivamente los tiempos de espera. | Puede aumentar el tiempo necesario para detectar un fallo definitivo. |
| Alertar sin reintentar | Permite identificar rápidamente los problemas. | Requiere intervención humana para recuperar el proceso. |

## 3. Decisión tomada

Se mantiene la estrategia de reintentos configurada en el pipeline, utilizando dos reintentos con un intervalo de 20 segundos, de acuerdo con la configuración definida en el proyecto.

Esta estrategia permite realizar nuevos intentos cuando se presenta un problema temporal de conexión con Kafka.

También se mantiene la dependencia entre las tareas del DAG, de manera que las actividades posteriores no continúen cuando una validación previa no se ha completado correctamente.

Durante las pruebas se comprobó que, después de recuperar Kafka y limpiar la tarea afectada junto con sus dependencias posteriores, las seis tareas del DAG finalizaron correctamente.

Para el caso de ausencia de datos RAW, la validación implementada en `rg_calidad.py` detectó que no existían archivos Parquet y generó una excepción `CalidadError`, evitando que el procesamiento continuara con información inexistente.

## 4. Evidencia utilizada

Se realizaron dos pruebas controladas para verificar el comportamiento del pipeline.

### Prueba 1. Interrupción del servicio Kafka

Se detuvo temporalmente el servicio Kafka mediante Docker Compose y se ejecutó el DAG desde Airflow.

La tarea afectada fue `check_fuentes`.

El registro mostró el siguiente error:

```text
ConnectionError: Kafka no disponible en kafka:9092
([Errno -5] No address associated with hostname)
```

Datos de la ejecución:

- DAG: rapidgo_flota_calidad.
- Tarea: check_fuentes.
- Fecha: 05/11/2026.
- Hora del fallo: 18:01:29 UTC.
- Run ID: manual__2026-11-05T17:57:25.533695+00:00.

Después se inició nuevamente Kafka y se realizó la limpieza de `check_fuentes` con la opción Downstream en Airflow. Como resultado, las seis tareas finalizaron correctamente.

### Prueba 2. Ausencia de archivos en RAW

Se renombró temporalmente la carpeta `datalake/raw` para simular la ausencia de archivos originales.

Durante la ejecución, Airflow registró:

```text
Archivos RAW detectados en check_fuentes: 0
```

Posteriormente, la tarea `validar_raw` generó el siguiente error:

```text
rg_calidad.CalidadError: RAW vacio: no hay archivos Parquet en /opt/airflow/datalake/raw/telemetry
```

Datos de la ejecución:

- DAG: rapidgo_flota_calidad.
- Tarea: validar_raw.
- Fecha: 05/11/2026.
- Hora del fallo: 20:05:29 UTC.
- Run ID: manual__2026-11-05T20:03:59.031310+00:00.
- Archivos RAW detectados: 0.
- Ruta evaluada: /opt/airflow/datalake/raw/telemetry.

La excepción `CalidadError` permitió identificar que no existían archivos Parquet en la ubicación esperada.

Después de la prueba, se debe restaurar la carpeta RAW y comprobar mediante una nueva ejecución que el pipeline finaliza correctamente.

### Resultados de recuperación

- Kafka: recuperación comprobada. Las seis tareas del DAG finalizaron correctamente después de iniciar nuevamente el servicio y limpiar las tareas dependientes.
- RAW: se debe registrar la ejecución exitosa posterior a la restauración de la carpeta original.
- Tiempo de recuperación: no se registró un tiempo total de recuperación en las evidencias disponibles.

## 5. Consecuencias / trade-offs

La estrategia implementada presenta las siguientes consecuencias:

- Los reintentos permiten gestionar algunos problemas temporales de conexión.
- La dependencia entre tareas evita que se generen resultados cuando las validaciones anteriores no se han completado correctamente.
- Cuando Kafka presenta una interrupción prolongada, puede ser necesaria la intervención para recuperar el servicio.
- La limpieza y el relanzamiento de las tareas en Airflow permiten continuar el procesamiento después de solucionar el problema.
- Las validaciones de calidad permiten detectar la ausencia de información en la capa RAW antes de continuar con las siguientes actividades.

## 6. Conclusión

Las pruebas realizadas permitieron comprobar la importancia de contar con validaciones de calidad y mecanismos de recuperación dentro del pipeline de RapidGo.

La interrupción de Kafka demostró que el sistema puede detectar problemas de conexión y detener las tareas que dependen de la disponibilidad de este servicio.

Por otra parte, la prueba de RAW vacío permitió comprobar que la validación de calidad identifica la ausencia de archivos Parquet y evita continuar con el procesamiento cuando no existe información disponible.

Finalmente, la recuperación de Kafka permitió comprobar que las seis tareas del DAG podían ejecutarse correctamente después de solucionar el problema.

Estas pruebas permitieron evaluar el comportamiento del sistema ante fallos controlados y documentar la estrategia de recuperación utilizada.