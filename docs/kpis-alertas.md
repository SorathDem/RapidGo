# KPIs y reglas de alerta: Variante 8

Los KPIs se calculan en Spark Structured Streaming por ventana de tiempo
(5 minutos en producción; 1 minuto en pruebas) y se guardan en
`datalake/curated/`.

## KPIs
| KPI | Fórmula | Ventana | Salida |
|---|---|---|---|
| Km acumulados por vehículo | `km_recorridos` = max(odometer_km) - min(odometer_km) por vehicle_id dentro de la ventana | 5 min (tumbling) | curated/kpis_vehiculo |
| Fallas por tipo | count(*) donde fault_code no es nulo, agrupado por fault_code | 5 min | curated/fallas_por_tipo |
| Vehículos en riesgo | `en_riesgo` = alerta A1 o alerta A2, calculado por vehículo y ventana | 5 min | curated/kpis_vehiculo y curated/alertas |

### Notas sobre los KPIs
- **Km acumulados:** se define como los kilómetros recorridos dentro de la
  ventana (máximo menos mínimo del odómetro), no el odómetro total.
- **Vehículos en riesgo:** Spark calcula la bandera `en_riesgo` por vehículo y
  por ventana. El total o el porcentaje de la flota en riesgo no se calcula en
  streaming (`countDistinct` no está soportado); lo calculará Airflow en la
  Semana 3 a partir de los Parquet de curated.

## Reglas de alerta (umbrales iniciales, ajustables)
| ID | Regla | Condición | Severidad |
|---|---|---|---|
| A1 | Mantenimiento vencido | km_since_last_service > 5000 (máximo observado en la ventana) | Media |
| A2 | Parámetro mecánico crítico | al menos `MIN_LECTURAS_CRITICAS` lecturas con engine_temp_c > 105 del mismo vehículo dentro de la ventana | Alta |

Los umbrales viven en `spark/config.py`. El valor de `MIN_LECTURAS_CRITICAS`
y la decisión de contar lecturas dentro de la ventana en lugar de lecturas
consecutivas están justificados con datos reales en
[ADR-002](decisions/ADR-002-calibracion-alerta-a2.md).

## Justificación de umbrales

### A1: Mantenimiento vencido (km_since_last_service > 5000)
El intervalo de 5.000 km es el estándar de mantenimiento preventivo para motos
en Colombia. Auteco lo recomienda y lo reporta Semana (2026), y el manual de
usuario de la MB Terra 2026 incluye una revisión programada a los 5.000 km en
su diagrama de mantenimiento de rutina. Superar ese intervalo implica un
mantenimiento atrasado.

Fuentes:
- Semana, "Cada cuánto hacer mantenimiento a la moto: revisión clave a los 5.000 km":
  https://www.semana.com/vehiculos/articulo/cada-cuanto-hacer-mantenimiento-a-la-moto-revision-clave-a-los-5000-km/202601/
- MB Motos, Manual de usuario Terra 2026:
  https://mbmotos.com/wp-content/uploads/2025/10/Manual_Terra-2026.pdf
- EnColombia, "¿Cada cuánto debes hacer mantenimiento a tu motocicleta?":
  https://encolombia.com/economia/tecno/mantenimiento-a-tu-motocicleta/

### A2: Parámetro crítico (engine_temp_c > 105)
Los motores de moto operan normalmente entre unos 65 y 110 °C, y el rango
ideal se ubica aproximadamente entre 82 y 104 °C. Se fijó 105 °C como umbral
porque está justo por encima de ese rango ideal. Como en tráfico urbano la
temperatura puede rozar ese valor sin que exista una falla, no basta una
lectura aislada: se exige un mínimo de lecturas críticas dentro de la ventana.
El código de falla P0217 corresponde a "Engine Coolant Over Temperature
Condition" (OBD-II).

Fuentes:
- Flow Racers, "How Hot Does A Motorcycle Cylinder Head Get?":
  https://flowracers.com/blog/motorcycle-cylinder-head-temperature/
- Riders Share, "How to Keep Your Motorcycle from Overheating in the Summer":
  https://riders-share.com/blog/article/how-to-keep-your-motorcycle-from-overheating
- RepairPal, "P0217 - OBD II Trouble Code":
  https://repairpal.com/OBD-II-Code-P0217

### Limitaciones y recalibración
Los umbrales dependen del modelo de moto y de si el motor es refrigerado por
aire o por líquido. La calibración del mínimo de lecturas de A2 con los datos
de la primera corrida está en el ADR-002 y deberá repetirse con más ejecuciones.