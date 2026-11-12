# Checkpoint Semana 3: v0.3.0-orchestration

**Equipo 6 · Variante 8 (Mantenimiento de flota)**

## 1. Qué quedó funcionando

DAG `rapidgo_flota_calidad` en Airflow (Docker, `http://localhost:8081`) con 6 tasks:
`check_fuentes → validar_raw → validar_processed → agregar_kpis → validar_curated → generar_reporte`.
Reintentos (`retries=2`, 20 s), paso de datos por XCom, reporte final en `reports/`,
CI ampliado que importa el DAG y ejecuta 9 pruebas de Airflow.

| Persona | Issue | Aporte | PR |
|---|---|---|---|
| P1 | #15 | DAG base, configuración compartida y `check_fuentes` | «COMPLETAR» |
| P2 | #21 | Airflow en docker-compose y KPIs de flota (`agregar_kpis`) | «COMPLETAR» |
| P3 | #16 | Validaciones de calidad, fallo controlado y ADR-003 | «COMPLETAR» |
| P4 | #17 | CI ampliado y reporte final | «COMPLETAR» |

## 2. Valor real observado en la ejecución

Ejecución exitosa del DAG (run `«COMPLETAR: run_id»`):
- Filas RAW: «COMPLETAR» · PROCESSED: «COMPLETAR» · CURATED: «COMPLETAR»
- Vehículos monitoreados: «COMPLETAR» · en riesgo: «COMPLETAR» («COMPLETAR» %)
- Alertas A1 (mantenimiento vencido): «COMPLETAR» · A2 (temperatura crítica): «COMPLETAR»
- Duración total del DAG: «COMPLETAR»
- Reporte generado: `reports/«COMPLETAR».md`

## 3. Decisión técnica tomada y por qué

«COMPLETAR: la decisión central de ADR-003 en 2 o 3 líneas, por ejemplo la política de reintentos.»
Evidencia y alternativas descartadas en [ADR-003](../decisions/).

## 4. Fallo controlado y recuperación

- **Tarea que falló:** `«COMPLETAR»` (provocado con «COMPLETAR: Kafka detenido / RAW renombrado»)
- **Mensaje real del log:**
  ```
  «COMPLETAR: pegar las líneas exactas del log»
  ```
- **Tareas afectadas:** «COMPLETAR: la que falló y las que quedaron en `upstream_failed`»
- **Comportamiento observado:** «COMPLETAR: reintentos y tiempo hasta el estado failed»
- **Recuperación:** «COMPLETAR: qué se hizo (por ejemplo `docker compose start kafka` y *Clear task* con downstream) y cuánto tardó»
- **Cambio que resolvió el problema:** «COMPLETAR: cambio de código o configuración, con enlace al PR»

## 5. Enlaces

- Issues: #15, #16, #17, #21
- PRs: «COMPLETAR»
- Actions (CI en verde): «COMPLETAR» · ciclo rojo → verde: «COMPLETAR»
- Ejecución exitosa en Airflow: «COMPLETAR: captura o ruta»
- Release: v0.3.0-orchestration