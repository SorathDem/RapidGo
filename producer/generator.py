"""Generador de telemetria de flota (Variante 8)."""
import random
import uuid
from datetime import datetime, timezone

TIPOS = ["moto", "moto", "moto", "carro"]
FALLAS = ["P0217", "P0128", "P0300", "P0420"]


def crear_flota(n_vehiculos, seed=None):
    if seed is not None:
        random.seed(seed)
    flota = {}
    for i in range(1, n_vehiculos + 1):
        vid = f"VEH-{i:03d}"
        flota[vid] = {
            "vehicle_id": vid,
            "vehicle_type": random.choice(TIPOS),
            "odometer_km": round(random.uniform(5000, 30000), 1),
            "km_since_last_service": round(random.uniform(3500, 5200), 1),
            "usage_hours_today": round(random.uniform(0, 4), 1),
            "hot_streak": 0,
        }
    return flota


def siguiente_evento(v):
    """Avanza el estado del vehiculo y devuelve un evento valido segun el contrato."""
    avance = random.uniform(0.5, 3.0)  # tiempo simulado acelerado
    v["odometer_km"] += avance
    v["km_since_last_service"] += avance
    v["usage_hours_today"] = min(24.0, v["usage_hours_today"] + random.uniform(0.01, 0.05))

    if v["hot_streak"] > 0:
        v["hot_streak"] -= 1
        temp = random.uniform(106, 115)
    elif random.random() < 0.03:
        v["hot_streak"] = random.randint(1, 3)
        temp = random.uniform(106, 115)
    else:
        temp = min(104.0, max(70.0, random.gauss(90, 5)))

    if temp > 105 and random.random() < 0.5:
        falla = random.choice(FALLAS)
    elif random.random() < 0.01:
        falla = random.choice(FALLAS)
    else:
        falla = None

    if v["km_since_last_service"] > 5500 and random.random() < 0.05:
        v["km_since_last_service"] = 0.0  # se hizo mantenimiento

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "vehicle_id": v["vehicle_id"],
        "vehicle_type": v["vehicle_type"],
        "odometer_km": round(v["odometer_km"], 1),
        "km_since_last_service": round(v["km_since_last_service"], 1),
        "engine_temp_c": round(temp, 1),
        "fault_code": falla,
        "usage_hours_today": round(v["usage_hours_today"], 2),
    }


def evento_invalido():
    """Eventos corruptos a proposito, para probar el manejo de rechazados en Spark."""
    return random.choice([
        {"event_id": "no-es-uuid", "vehicle_id": "", "odometer_km": -50, "engine_temp_c": "caliente"},
        {"vehicle_id": "VEH-001"},
    ])