# Checkpoint Semana 3: v0.3.0-orchestration

**Equipo 6 · Variante 8 (Mantenimiento de flota)**

## 1. Qué quedó funcionando

DAG `rapidgo_flota_calidad` en Airflow (Docker, `http://localhost:8081`) con 6 tasks:
`check_fuentes → validar_raw → validar_processed → agregar_kpis → validar_curated → generar_reporte`.
Reintentos (`retries=2`, 20 s), paso de datos por XCom, reporte final en `reports/`,
CI ampliado que importa el DAG con `DagBag` y ejecuta 6 pruebas de Airflow (17 pruebas en total, todas en verde).

| Persona | Issue | Aporte | PR |
|---|---|---|---|
| P1 | #15 | DAG base, configuración compartida y `check_fuentes` | #42 feat: implementar DAG base de Airflow para mantenimiento de flota |
| P2 | #21 | Airflow en docker-compose y KPIs de flota (`agregar_kpis`) | #43 Airflow en docker-compose |
| P3 | #16 | Validaciones de calidad, fallo controlado y ADR-003 | #45 feat: validaciones de calidad de datos RAW, PROCESSED y CURATED |
| P4 | #17 | CI ampliado y reporte final | #44 ci: valida importacion del DAG y agrega reporte final (#17) |

## 2. Valor real observado en la ejecución

Ejecución exitosa del DAG (run `manual__2026-11-12T04:33:37.355833+00:00`, las 6 tasks en `success`). Es una corrida de verificación del 11/11/2026 (hora Colombia) con el código de v0.3.0, con datos reales generados por Spark (producer → Kafka → Spark Structured Streaming → `datalake/`):
- Filas RAW: 44545 · PROCESSED: 43659 · CURATED: 10 (una fila por vehículo en `curated/kpis_flota`)
- Vehículos monitoreados: 10 · en riesgo: 10 (100.0 %)
- Alertas A1 (mantenimiento vencido): 10 · A2 (temperatura crítica): 10
- Fallas por tipo: P0128 = 597 · P0217 = 548 · P0300 = 549 · P0420 = 588
- Archivos detectados por `check_fuentes`: 27 Parquet en RAW y 30 en PROCESSED
- Duración total del DAG: 75,6 s (en cola 2,3 s; ejecución 73,2 s; inicio 04:33:39 UTC y fin 04:34:52 UTC)
- Reporte generado: `reports/reporte_20261112_043451.md` (más `reports/ultimo_reporte.json`)

**Lectura del resultado:** RAW tiene 886 filas más que PROCESSED; son los eventos inválidos que Spark envía a `rejected/` (unos 2 %). El 100 % de vehículos en riesgo no es un error del DAG. `agregar_kpis` agrega por vehículo sobre todo el histórico, no por ventana, así que con más de 4000 lecturas por vehículo todos superan tanto el umbral de A2 (≥ 2 lecturas > 105 °C) como el de A1 (> 5000 km desde el servicio). Esto confirma lo que advertía el [ADR-002](../decisions/ADR-002-calibracion-alerta-a2.md): con un umbral absoluto de 2 lecturas la alerta pierde poder de discriminación, y conviene pasar a una proporción o calcular sobre una ventana.

## 3. Decisión técnica tomada y por qué

Se mantienen **reintentos fijos (`retries=2`, `retry_delay=20 s`)** y la dependencia estricta entre tasks. Es lo más simple y cubre cortes cortos de conexión con Kafka. Además, ninguna task posterior se ejecuta si falla una validación previa, así que no se generan KPIs ni reportes con datos faltantes. Se descartaron el backoff exponencial (alarga el tiempo para detectar un fallo definitivo) y alertar sin reintentar (siempre exige intervención humana). La desventaja aceptada es que, si Kafka se cae por mucho tiempo, hay que recuperar el servicio a mano y relanzar las tasks.
Evidencia y alternativas descartadas en [ADR-003](../decisions/ADR-003-orquestacion-recuperacion.md).

## 4. Fallo controlado y recuperación

- **Tarea que falló:** `check_fuentes` (provocado con Kafka detenido: `docker compose stop kafka`). En una segunda prueba se renombró `datalake/raw` y falló `validar_raw`.
- **Mensaje real del log:**
  ```
  ConnectionError: Kafka no disponible en kafka:9092
  ([Errno -5] No address associated with hostname)
  ```
  Segunda prueba (RAW renombrado, run `manual__2026-11-05T20:03:59.031310+00:00`, fallo a las 20:05:29 UTC):
  ```
  Archivos RAW detectados en check_fuentes: 0
  rg_calidad.CalidadError: RAW vacio: no hay archivos Parquet en /opt/airflow/datalake/raw/telemetry
  ```
- **Tareas afectadas:** `check_fuentes` en `failed`. Según la estructura del DAG (trigger rule por defecto `all_success`), las 5 tasks posteriores (`validar_raw`, `validar_processed`, `agregar_kpis`, `validar_curated` y `generar_reporte`) quedan en `upstream_failed`.

- **Comportamiento observado:** `check_fuentes` se intentó 3 veces (1 intento y 2 reintentos, con 20 s entre cada uno) antes de quedar en `failed`. El run se creó a las 17:57:25 UTC y el fallo definitivo se registró a las 18:01:29 UTC, unos 4 minutos después de crear el run (run `manual__2026-11-05T17:57:25.533695+00:00`, 05/11/2026).
- **Recuperación:** se volvió a iniciar Kafka (`docker compose start kafka`) y se aplicó *Clear task* sobre `check_fuentes` con la opción *Downstream*; las 6 tasks terminaron en `success`. En la prueba de RAW se restauró la carpeta `datalake/raw` y se volvió a ejecutar el DAG con éxito (PR #45). 

- **Cambio que resolvió el problema:** la recuperación fue operativa y no requirió cambios de código. Lo que sí quedó en código fueron las validaciones de calidad de `rg_calidad.py` (`CalidadError` cuando RAW está vacío, esquema de PROCESSED, nulos en `vehicle_id`, consistencia de CURATED) y el ADR-003, en el PR #45. La política de reintentos (`retries=2`, 20 s) viene de `rg_config.py`, en el PR #42.

## 5. Enlaces

- Issues: #15, #16, #17, #21
- PRs: #42 (DAG base), #43 (Airflow en Docker y KPIs), #44 (CI ampliado y reporte), #45 (calidad y ADR-003), #46 y #48 (`dev` → `main`), #49 (estabilización con clon limpio, issue #23)
- Actions (CI en verde): [42129778939](https://github.com/SorathDem/RapidGo/actions/runs/42129778939) (push a `main`, `DAG OK: ['rapidgo_flota_calidad']`, `17 passed`) · ciclo rojo → verde: rojo real [41950041378](https://github.com/SorathDem/RapidGo/actions/runs/41950041378) (`ModuleNotFoundError: No module named 'rg_config'` al validar el DAG con `DagBag`, corregido con `PYTHONPATH` en el commit `3677014`) → verde [41952194331](https://github.com/SorathDem/RapidGo/actions/runs/41952194331); rojo provocado [42039070160](https://github.com/SorathDem/RapidGo/actions/runs/42039070160) (`AssertionError: falta validar_raw >> validar_processed`, `1 failed, 14 passed`) → verde con el revert [42039855227](https://github.com/SorathDem/RapidGo/actions/runs/42039855227) (todo en el PR #44)
- Ejecución exitosa en Airflow: run `manual__2026-11-05T17:57:25.533695+00:00` (recuperación del fallo de Kafka) y run `manual__2026-11-12T04:33:37.355833+00:00` (corrida de verificación con datos reales, reporte `reports/reporte_20261112_043451.md`). DAG `rapidgo_flota_calidad`.
- Release: [v0.3.0-orchestration](https://github.com/SorathDem/RapidGo/releases/tag/v0.3.0-orchestration) (tag sobre el commit `25c0d78`, merge del PR #48 a `main` del 05/11/2026; publicado el 11/11/2026)
