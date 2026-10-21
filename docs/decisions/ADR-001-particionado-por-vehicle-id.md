# ADR-001 · Particionado del topic de telemetría por `vehicle_id`

- **Estado:** Aceptado
- **Fecha:** 2026-10-21
- **Decide:** Persona 4 (estructura del repo, Docker, requirements, checkpoint)
- **Ámbito:** topic `rapidgo.fleet.telemetry`

## Contexto

El topic `rapidgo.fleet.telemetry` transporta telemetría de flota. El contrato de evento ([`../contrato-evento.md`](../contrato-evento.md)) define `vehicle_id` como clave natural de la entidad: un mismo vehículo emite eventos de forma continua (odómetro, temperatura, fallas, horas de uso).

Los KPI del proyecto dependen del orden:
- **A1** (mantenimiento vencido): umbral sobre `km_since_last_service`.
- **A2** (parámetro crítico): `engine_temp_c > 105` en **2 o más eventos consecutivos** ([`../kpis-alertas.md`](../kpis-alertas.md)).

A2 es, por definición, un cálculo *consecutive* sobre la serie de un mismo vehículo. Sin afinidad por `vehicle_id`, dos lecturas consecutivas pueden caer en particiones distintas y el cálculo exige un shuffle costoso (o queda incorrecto) en cada micro-batch.

## Decisión

**La key del topic es `vehicle_id`.** Kafka hashea la key para asignar partición, de modo que **todos los eventos de un mismo vehículo van a la misma partición**.

**El topic se crea con 3 particiones**, alineado con `KAFKA_NUM_PARTITIONS=3` en el [`docker-compose.yml`](../../docker-compose.yml).

## Consecuencias

**Positivas**
- Orden causal preservado **por vehículo** sin depender de timestamps.
- El estado por vehículo (ventana consecutiva de A2, contador de A1) es *local a la partición*: no requiere shuffle entre particiones en el join ni en la agregación.
- Replicación y rebalanceo coherentes: una unidad de estado = una partición.

**Negativas / aceptadas en v1**
- El paralelismo de consumidores queda topado en **3 particiones**; con más workers que particiones habrá workers ociosos.
- Riesgo de *hot partition* si un `vehicle_id` muy activo concentra eventos. Aceptado en v1: se vigila en el dashboard de lagging y se revisa en Semana 2.
- El número de particiones **no se puede reducir sin recrear el topic**; crecerlo sí es posible, pero rehashifica keys existentes (cambia la partición de un `vehicle_id`) y reinicia el estado por vehículo.

**Alternativas consideradas**
- *Key = `event_id`*: particionamiento uniforme, pero rompe el orden por vehículo y hace inviable A2 sin shuffle.
- *Más particiones (p. ej. 12) + estado externo (Redis/DB)*: más paralelismo, a costa de introducir estado externo y su consistencia — no justificado en v1.

## Contexto operativo

`KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092` (configuración actual del compose) hace que el broker anuncie `localhost`; dentro de la red de Docker los clientes resolverían ese `localhost` como el propio contenedor. Resolver antes de validar el particionado en runtime — ver [Checkpoint Semana 1 · Riesgo principal](../checkpoints/semana1.md).

## Referencias

- Contrato del evento: [`../contrato-evento.md`](../contrato-evento.md)
- Schema: [`../evento.schema.json`](../evento.schema.json)
- Umbrales y fuentes: [`../kpis-alertas.md`](../kpis-alertas.md)
- Configuración del broker: [`../../docker-compose.yml`](../../docker-compose.yml)
