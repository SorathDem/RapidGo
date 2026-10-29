"""Reglas de alerta en Python puro (probables sin Spark). Umbrales en spark/config.py."""
from spark import config as C


def mantenimiento_vencido(km_since_last_service):
    """A1: mantenimiento vencido."""
    return km_since_last_service > C.UMBRAL_KM_SERVICIO


def temperatura_critica(temperaturas):
    """A2: al menos MIN_LECTURAS_CRITICAS lecturas sobre el umbral en la ventana."""
    return sum(t > C.UMBRAL_TEMP_C for t in temperaturas) >= C.MIN_LECTURAS_CRITICAS