## Justificación de umbrales

### A1: Mantenimiento vencido (km_since_last_service > 5000)
El intervalo de 5.000 km es el estándar de mantenimiento preventivo para motos
en Colombia. Auteco lo recomienda y lo reporta Semana (2026), y el manual de
usuario de la MB Terra 2026 incluye una revisión programada a los 5.000 km en
su diagrama de mantenimiento de rutina. Se usa como umbral de alerta porque
superar ese intervalo implica un mantenimiento atrasado.

Fuentes:
- Semana, "Cada cuánto hacer mantenimiento a la moto: revisión clave a los 5.000 km":
  https://www.semana.com/vehiculos/articulo/cada-cuanto-hacer-mantenimiento-a-la-moto-revision-clave-a-los-5000-km/202601/
- MB Motos, Manual de usuario Terra 2026:
  https://mbmotos.com/wp-content/uploads/2025/10/Manual_Terra-2026.pdf
- EnColombia, "¿Cada cuánto debes hacer mantenimiento a tu motocicleta?":
  https://encolombia.com/economia/tecno/mantenimiento-a-tu-motocicleta/

### A2: Parámetro crítico (engine_temp_c > 105 en 2 o más eventos consecutivos)
Los motores de moto operan normalmente entre unos 65 y 110 °C, y el rango
ideal se ubica aproximadamente entre 82 y 104 °C. Se fijó 105 °C como umbral
porque está justo por encima de ese rango ideal. Como en tráfico urbano el
ventilador del motor se activa alrededor de 105 °C y la temperatura puede
rozar ese valor sin que exista una falla, se exigen 2 lecturas consecutivas
para evitar falsos positivos por un pico aislado. El código de falla P0217
corresponde a "Engine Coolant Over Temperature Condition" (OBD-II).

Fuentes:
- Flow Racers, "How Hot Does A Motorcycle Cylinder Head Get?":
  https://flowracers.com/blog/motorcycle-cylinder-head-temperature/
- Riders Share, "How to Keep Your Motorcycle from Overheating in the Summer":
  https://riders-share.com/blog/article/how-to-keep-your-motorcycle-from-overheating
- RepairPal, "P0217 - OBD II Trouble Code":
  https://repairpal.com/OBD-II-Code-P0217

### Limitaciones y recalibración
Los umbrales dependen del modelo y de si el motor es refrigerado por aire o
por líquido. Se recalibrarán en la Semana 2 con los valores reales generados
por el producer y los resultados observados.