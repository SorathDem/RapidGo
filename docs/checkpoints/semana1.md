# Checkpoint · Semana 1 — RapidGo · 2026-10-21 · Persona 4

## 1. Qué quedó funcionando
- **Estructura**: `.github/workflows`, `airflow/dags`, `producer`, `spark`, `tests`, `samples`, `docs/checkpoints`, `docs/decisions` (con `.gitkeep`); `datalake/` sin `.gitkeep`. `.gitignore` con los 12 patrones acordados.
- **`requirements.txt`**: `pyspark==3.5.3`, `kafka-python==2.0.2`, `pytest==8.3.3`, `jsonschema==4.23.0`, `pandas==2.2.3`, `pyarrow==17.0.0`.
- **`docker-compose.yml`**: `kafka` (`apache/kafka:3.8.0`) y `spark-master` (`apache/spark:3.5.3`; 8080/7077; volúmenes `./spark`, `./datalake`).
- **Contrato**: `docs/contrato-evento.md` + `docs/evento.schema.json` (9 campos obligatorios, `additionalProperties: false`).
- **Samples y validador**: 3 muestras en `samples/` (válida, con falla `P0217`, inválida) + `validar_samples.py`.
- **KPIs**: `docs/kpis-alertas.md` (A1 = 5000 km; A2 = 105 °C en 2+ eventos consecutivos), con fuentes citadas.
- **Pendiente**: producer, jobs de Spark, checkpoint de streaming, tests, DAGs de Airflow y workflows de CI (carpetas vacías).

## 2. Valor o evidencia real observada
**Validador de samples**
```
python validar_samples.py
evento_valido -> OK
evento_con_falla -> OK
evento_invalido -> RECHAZADO: 'event_time' is a required property
```
**Kafka — `kafka-topics.sh --describe`**: topic verificado por P1 (salida pegada abajo); no verificado en esta máquina.
```
Topic: rapidgo.fleet.mantenimiento.v1	TopicId: N4IDO_JPTgSrYtJzwpfqYw	PartitionCount: 4	ReplicationFactor: 1	Configs: 
	Topic: rapidgo.fleet.mantenimiento.v1	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: rapidgo.fleet.mantenimiento.v1	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	Topic: rapidgo.fleet.mantenimiento.v1	Partition: 2	Leader: 1	Replicas: 1	Isr: 1
	Topic: rapidgo.fleet.mantenimiento.v1	Partition: 3	Leader: 1	Replicas: 1	Isr: 1
```
**`docker compose config --quiet`** — sin salida, valida. · **`docker compose ps`** (2026-10-21, tras `docker compose up -d`):
```
NAME           IMAGE                COMMAND                  SERVICE        CREATED         STATUS        PORTS
kafka          apache/kafka:3.8.0   "/__cacert_entrypoin…"   kafka          2 seconds ago   Up 1 second   0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp
spark-master   apache/spark:3.5.3   "/opt/entrypoint.sh …"   spark-master   2 seconds ago   Up 1 second   0.0.0.0:7077->7077/tcp, [::]:7077->7077/tcp, 0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp
```

## 3. Decisión técnica más importante
**Key del topic = `vehicle_id`.** ADR: [PENDIENTE: ADR-001, lo define el equipo]. Con la key en `vehicle_id` todos los eventos de un vehículo caen en la misma partición → orden causal por vehículo y estado por vehículo (KPI A2) local a la partición, sin shuffle entre particiones.

[CONFIRMAR CON EL EQUIPO: el plan decía 3, el topic real tiene 4]

## 4. Riesgo principal y plan de la Semana 2
**Riesgos:** (1) Python 3.13 + `pyarrow==17.0.0` sin wheel → pip compila desde fuente y falla con `ModuleNotFoundError: No module named 'pkg_resources'`; (2) compose sin listener interno → Spark en contenedor no alcanza a Kafka; (3) particiones (3 vs 4) y nombre de topic (`rapidgo.fleet.telemetry` en el contrato vs `rapidgo.fleet.mantenimiento.v1` real).

**Acciones:** fijar Python 3.11/3.12 en el CI; agregar el listener interno al compose; confirmar con el equipo particiones y nombre de topic.
**Plan:** P1 producer · P2 Spark · P3 ventana y alertas · P4 tests y CI.

## 5. Enlaces
**Issues** (total: 22): https://github.com/SorathDem/RapidGo/issues · https://github.com/SorathDem/RapidGo/issues/8 · https://github.com/SorathDem/RapidGo/issues/9 · https://github.com/SorathDem/RapidGo/issues/10

**PRs** (integran en `dev`, no en `main`): https://github.com/SorathDem/RapidGo/pulls?q=is%3Apr+state%3Aclosed — [PENDIENTE: números de PR, `gh` no disponible en este entorno]
**Releases**: https://github.com/SorathDem/RapidGo/releases — [PENDIENTE: release `v0.1.0-foundation` no verificado]
