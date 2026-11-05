# ADR-002 - Calibración de la alerta A2 (temperatura crítica)

## Problema
La regla A2 marca un vehículo como en riesgo cuando, dentro de una ventana,
tiene al menos MIN_LECTURAS_CRITICAS lecturas con engine_temp_c > 105 °C.
El valor inicial era 2, elegido sin datos. Además, el diseño original pedía
lecturas consecutivas, pero detectarlas exige guardar estado por vehículo en
Spark Streaming, mucho más complejo que contar dentro de la ventana.

## Opciones consideradas
| Opción | Ventaja | Desventaja |
|---|---|---|
| Consecutivas con estado por vehículo | Filtra picos aislados | Complejidad alta, estado a mantener y recuperar |
| Conteo en ventana, mínimo 2 (inicial) | Simple | Marca casi todo (ver evidencia) |
| Conteo en ventana, mínimo 8 | Simple y separa el grupo anómalo | Depende del tamaño de ventana |
| Proporción de lecturas críticas >= 15 % | No depende del tamaño de ventana | Requiere cambiar el cálculo |

## Decisión tomada
Contar lecturas críticas dentro de la ventana, sin consecutivas. Para la
ventana de prueba de 1 minuto, subir MIN_LECTURAS_CRITICAS de 2 a 8. Para la
ventana final de 5 minutos se propone expresar la regla como proporción
(>= 15 % de las lecturas de la ventana), porque un conteo absoluto no se
traslada entre tamaños de ventana.
(Pendiente de acordar con el equipo antes de cambiar spark/config.py.)

## Evidencia utilizada
Corrida con ventana de 1 minuto, watermark de 30 s y 1500 eventos enviados
(1468 procesados + 32 rechazados = 1500, sin pérdida). Resultado: 21 filas
vehículo-ventana.

| Mínimo de lecturas críticas | Filas marcadas |
|---|---|
| 2 (actual) | 17 de 21 (81 %) |
| 3 | 15 |
| 4 | 12 |
| 5 | 9 |
| 6 | 7 |
| 8 | 6 (29 %) |
| 10 | 6 |

Los vehículos VEH-009, 006, 007, 008 y 003 concentran entre 17,5 % y 27,1 % de
lecturas críticas por ventana; el resto queda en 13,2 % o menos. Con mínimo 8
y mínimo 10 el resultado es el mismo, lo que indica que el grupo anómalo
queda bien separado.

## Consecuencias / trade-offs
- Con mínimo 2, la alerta pierde utilidad: casi todo vehículo está en riesgo.
- Perdemos la garantía de "consecutivas": una ráfaga de calor aislada al
  inicio y otra al final de la ventana cuentan igual que una continua.
- La muestra es pequeña (21 filas, una corrida, datos simulados); el valor
  debe recalibrarse con más ejecuciones.
- Cambiar el umbral exige tocar spark/config.py, archivo de P2.