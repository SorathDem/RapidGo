# Checkpoint Semana 2: v0.2.0-streaming

**Equipo 6 · Variante 8 (Mantenimiento de flota)**

## 1. Qué quedó funcionando

Pipeline Producer → Kafka → Spark Structured Streaming → Data Lake (Parquet) con CI activo.

| Persona | Issue | Qué hizo | PR |
|---|---|---|---|
| P1 | #11 | Producer en Python que genera telemetría válida según el contrato, con key `vehicle_id`, un porcentaje de eventos inválidos a propósito y header `equipo` para identificar el origen. Publicó el release. | #28 feat: producer de telemetría de flota con key vehicle_id (#11) |
| P2 | #12 | Job de Spark que lee el topic, parsea el JSON con esquema explícito, valida contra el contrato, aplica 4 transformaciones y escribe `raw/`, `processed/` y `rejected/` en Parquet con checkpoint. | #29 feat(spark): implementar procesamiento de telemetría con Spark Streaming |
| P3 | #13 | Agregación por ventana (KPIs por vehículo y fallas por tipo), alertas A1 y A2 en `curated/`, reglas en `spark/rules.py` y ADR-002. | #40 feat: agregaciones por ventana, alertas A1/A2 y ADR-002 (#13) (reemplaza al #31, enviado a `main` por error y revertido en #32; reglas en #30) |
| P4 | #14 | `ci.yml` (compileall, pytest, docker compose config) activo en push, pull_request y workflow_dispatch; tests de contrato, reglas y generador; corrección de `requirements.txt`. | #36 ci: workflow de CI y tests de contrato, reglas y generador (#14) (base de tests y `requirements.txt` en #35) |

## 2. Valor real observado en la ejecución

- **Distribución por partición (P1):** el vehículo `VEH-008` cayó siempre en la partición 2 (4 de 4 eventos, offsets consecutivos dentro de la partición). Salida real del producer:
  ```
  #2 key=VEH-008 -> particion=2 offset=21012
  #6 key=VEH-008 -> particion=2 offset=21015
  #7 key=VEH-008 -> particion=2 offset=21016
  #9 key=VEH-008 -> particion=2 offset=21017
  ```
  Lo mismo pasó con `VEH-004`, que salió 2 veces y las dos cayó en la partición 0 (offsets 8591 y 8592). Las 3 particiones del topic recibieron eventos: la 0 (VEH-004), la 1 (VEH-003 y VEH-007) y la 2 (VEH-008, VEH-009 y VEH-010).
- **Eventos procesados y rechazados (P2):** 1468 procesados y 32 rechazados de 1500 enviados (1468 + 32 = 1500, sin pérdida). Corrida del PR #40 con ventana de 1 minuto. En una validación anterior (PR #29, varias corridas de 200 eventos) el Data Lake acumuló 635 procesados y 10 rechazados.
- **Primer resultado en `curated/` (P3):** apareció en pocos minutos, no quedó registrados los minutos después de iniciar, con ventana de 1 minuto y watermark de 30 s (valores de prueba; en `spark/config.py` los valores por defecto son 5 minutos y 1 minuto). Se generaron `curated/kpis_vehiculo` (21 filas vehículo-ventana), `curated/fallas_por_tipo` y `curated/alertas`. Alertas detectadas: A1 = , A2 = 17 de 21 filas vehículo-ventana (81 %) con el umbral actual de 2 lecturas críticas; con un umbral de 8 serían 6 de 21 (29 %).
- **Checkpoint de Spark (P3):** al reiniciar el job no reprocesó datos: después de detenerlo y reiniciarlo, el conteo de `processed` siguió en 1468 (PR #40). Offsets registrados en `checkpoints/processed/offsets`: 
- **Tests (P4):** `pytest -v` ejecutó 11 pruebas, todas en verde (`11 passed`, Actions run [40824165123](https://github.com/SorathDem/RapidGo/actions/runs/40824165123)).

## 3. Decisión técnica tomada y por qué

**A2 cuenta las lecturas críticas dentro de la ventana, no las consecutivas** (ADR-002). Detectar lecturas consecutivas obliga a mantener y recuperar estado por vehículo en Spark Streaming, lo que es mucho más complejo; contar dentro de la ventana es simple. Con la evidencia (1500 eventos, 21 filas vehículo-ventana) se vio que el umbral inicial de 2 lecturas marca en riesgo al 81 % de las filas (17 de 21), así que la alerta pierde utilidad. Con 8 lecturas se marca el 29 % (6 de 21). Con 8 y con 10 el resultado es el mismo, lo que indica que el grupo anómalo (VEH-009, 006, 007, 008 y 003, con entre 17,5 % y 27,1 % de lecturas críticas) queda bien separado. Para la ventana final de 5 minutos se propone expresar la regla como proporción (≥ 15 % de las lecturas de la ventana), porque un conteo absoluto no se traslada entre tamaños de ventana.

**Lo que se pierde:** la garantía de "consecutivas" (una ráfaga aislada al inicio y otra al final de la ventana cuentan igual que una continua). La muestra es pequeña (una corrida, datos simulados). El cambio de umbral en `spark/config.py` quedó pendiente de acordar con el equipo; hoy sigue en `MIN_LECTURAS_CRITICAS = 2`.

Detalle y evidencia en [ADR-002](../decisions/ADR-002-calibracion-alerta-a2.md).

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

**Solución:** entorno virtual con `pyspark==3.5.3`, `pandas==2.2.3` y `kafka-python-ng`; Java 17; `winutils.exe` en `C:\hadoop\bin` y `HADOOP_HOME` configurado. `kafka-python-ng` quedó en `requirements.txt` con el PR #35 (`chore: base de tests, pytest.ini y requirements con kafka-python-ng`), y la necesidad de `winutils.exe` y `HADOOP_HOME` quedó anotada en la nota para el revisor del PR #40. 

**Otro problema (P4): CI en rojo.**
- **Fallo provocado (Gate):** en el PR #36 se rompió a propósito el sample `evento_valido` (commit `ca06ee2`). El CI falló ([run 40823964659](https://github.com/SorathDem/RapidGo/actions/runs/40823964659)) con:
  ```
  FAILED tests/test_contrato.py::test_samples_validos[evento_valido] - jsonschema.exceptions.ValidationError: 'VEH-7' does not match '^VEH-\\d{3}$'
  ========================= 1 failed, 10 passed in 4.14s =========================
  ```
  El revert (`64887a9`) lo dejó en verde: `11 passed` ([run 40824165123](https://github.com/SorathDem/RapidGo/actions/runs/40824165123)).
- **Fallo real:** en el PR #37 (`dev` → `main`) la recolección de pruebas falló ([run 40874904468](https://github.com/SorathDem/RapidGo/actions/runs/40874904468)):
  ```
  E   ModuleNotFoundError: No module named 'producer.generator'
  E   ImportError: cannot import name 'config' from 'spark' (unknown location)
  !!!!!!!!!!!!!!!!!!! Interrupted: 2 errors during collection !!!!!!!!!!!!!!!!!!!!
  ```
  El PR #38 intentó corregirlo con `PYTHONPATH` y `python -m pytest`, pero el CI siguió en rojo ([run 40878160413](https://github.com/SorathDem/RapidGo/actions/runs/40878160413)). El PR #39 revirtió ese cambio y restauró `dev`, y el CI volvió a verde con `11 passed` ([run 40880622456](https://github.com/SorathDem/RapidGo/actions/runs/40880622456)), lo que permitió mergear el #37.

## 5. Enlaces

- Issues: #11, #12, #13, #14
- PRs: #28 (producer), #29 (Spark Streaming), #30 (reglas A1/A2), #40 (ventanas, alertas y ADR-002; antes #31, revertido en #32), #35 y #36 (tests y CI), #38 y #39 (intento de corrección del CI y su revert), #34 y #37 (`dev` → `main`)
- Actions (CI en rojo y luego en verde): rojo [40823964659](https://github.com/SorathDem/RapidGo/actions/runs/40823964659) → verde [40824165123](https://github.com/SorathDem/RapidGo/actions/runs/40824165123) (fallo provocado, PR #36); rojo [40874904468](https://github.com/SorathDem/RapidGo/actions/runs/40874904468) → verde [40880622456](https://github.com/SorathDem/RapidGo/actions/runs/40880622456) (fallo real de importación, PR #37/#39)
- Ejecución manual (workflow_dispatch): el trigger quedó configurado en ci.yml desde el PR #36, pero en la Semana 2 no se lanzó ninguna ejecución manual. La primera se hizo el 11/11/2026 sobre main: [run 43149297271](https://github.com/SorathDem/RapidGo/actions/runs/43149297271), en verde.

- Release: [v0.2.0-streaming](https://github.com/SorathDem/RapidGo/releases/tag/v0.2.0-streaming) (tag sobre el commit `069ce4e`, merge del PR #40 del 29/10/2026; publicado el 11/11/2026)

