# Checkpoint Semana 2: v0.2.0-streaming

**Equipo 6 · Variante 8 (Mantenimiento de flota)**

## 1. Qué quedó funcionando

Pipeline Producer → Kafka → Spark Structured Streaming → Data Lake (Parquet) con CI activo.

| Persona | Issue | Qué hizo | PR |
|---|---|---|---|
| P1 | #11 | Producer en Python que genera telemetría válida según el contrato, con key `vehicle_id`, un porcentaje de eventos inválidos a propósito y header `equipo` para identificar el origen. Publicó el release. | «COMPLETAR» |
| P2 | #12 | Job de Spark que lee el topic, parsea el JSON con esquema explícito, valida contra el contrato, aplica 4 transformaciones y escribe `raw/`, `processed/` y `rejected/` en Parquet con checkpoint. | «COMPLETAR» |
| P3 | #13 | Agregación por ventana (KPIs por vehículo y fallas por tipo), alertas A1 y A2 en `curated/`, reglas en `spark/rules.py` y ADR-002. | «COMPLETAR» |
| P4 | #14 | `ci.yml` (compileall, pytest, docker compose config) activo en push, pull_request y workflow_dispatch; tests de contrato, reglas y generador; corrección de `requirements.txt`. | «COMPLETAR» |

## 2. Valor real observado en la ejecución

- **Distribución por partición (P1):** el vehículo `VEH-«COMPLETAR»` cayó siempre en la partición «COMPLETAR» (salida del producer: `key=VEH-... -> particion=N offset=N`).
- **Eventos procesados y rechazados (P2):** «COMPLETAR» procesados y «COMPLETAR» rechazados de «COMPLETAR» enviados.
- **Primer resultado en `curated/` (P3):** apareció «COMPLETAR» minutos después de iniciar, con ventana de «COMPLETAR» y watermark de «COMPLETAR». Alertas detectadas: A1 = «COMPLETAR», A2 = «COMPLETAR».
- **Checkpoint de Spark (P3):** al reiniciar el job no reprocesó datos; offsets registrados en `checkpoints/processed/offsets`: «COMPLETAR».
- **Tests (P4):** `pytest -v` ejecutó «COMPLETAR» pruebas, todas en verde.

## 3. Decisión técnica tomada y por qué

«COMPLETAR con la decisión del ADR-002.» Candidatos reales de este equipo:
- **A2 por conteo dentro de la ventana** (≥ 2 lecturas > 105 °C) en lugar de lecturas consecutivas, porque lo segundo exige estado por vehículo en streaming y aumenta la complejidad.
- **Spark local con `pyspark` en vez de contenedor**, para evitar la configuración de red entre contenedores y Kafka.

Detalle y evidencia en [ADR-002](../decisions/).

## 4. Problema real encontrado y cómo se resolvió

**Problema:** al ejecutar `python -m spark.streaming_job` en Windows, Spark no arrancó. Log real:

```
java.lang.RuntimeException: java.io.FileNotFoundException:
HADOOP_HOME and hadoop.home.dir are unset.
```

Además, `python -m producer.producer` falló con `ModuleNotFoundError: No module named 'kafka'`.

**Causas identificadas:**
- Falta de `winutils.exe` y de la variable `HADOOP_HOME` en Windows.
- El entorno tenía PySpark 4.x, mientras el job usa el conector `spark-sql-kafka-0-10_2.12:3.5.3`.
- `kafka-python-ng` no estaba instalado en el entorno.

**Solución:** entorno virtual con `pyspark==3.5.3`, `pandas==2.2.3` y `kafka-python-ng`; Java 17; `winutils.exe` en `C:\hadoop\bin` y `HADOOP_HOME` configurado. «COMPLETAR: PR o commit que dejó esto documentado en el README.»

**Otro problema (P4), si aplica:** «COMPLETAR: CI en rojo con el error real y el PR que lo corrigió.»

## 5. Enlaces

- Issues: #11, #12, #13, #14
- PRs: «COMPLETAR»
- Actions (CI en rojo y luego en verde): «COMPLETAR»
- Ejecución manual (`workflow_dispatch`): «COMPLETAR»
- Release: v0.2.0-streaming