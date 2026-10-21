# Contrato del evento: telemetría de flota

Topic: `rapidgo.fleet.telemetry` · Key: `vehicle_id` · Formato: JSON (UTF-8)

| Campo | Tipo | Obligatorio | Descripción | Regla |
|---|---|---|---|---|
| event_id | string (UUID) | Sí | Identificador único del evento | UUID válido |
| event_time | string (ISO-8601 UTC) | Sí | Momento de la lectura | Formato `YYYY-MM-DDTHH:MM:SSZ` |
| vehicle_id | string | Sí | Identificador del vehículo | Patrón `VEH-\d{3}` |
| vehicle_type | string | Sí | Tipo de vehículo | `moto`, `bicicleta`, `carro` |
| odometer_km | number | Sí | Kilometraje total | ≥ 0 |
| km_since_last_service | number | Sí | Km desde el último mantenimiento | ≥ 0 |
| engine_temp_c | number | Sí | Temperatura del motor (°C) | -20 a 200 |
| fault_code | string o null | Sí | Código de falla OBD-II | `null` o patrón `P\d{4}` |
| usage_hours_today | number | Sí | Horas de uso en el día | 0 a 24 |

## Notas
- `fault_code` es `null` cuando el vehículo no reporta falla.
- Un evento que no cumple estas reglas se considera inválido y se descarta.