from producer.generator import crear_flota, siguiente_evento
from tests.helpers import validar


def test_eventos_generados_cumplen_el_contrato():
    flota = crear_flota(5, seed=42)
    for _ in range(200):
        for v in flota.values():
            validar(siguiente_evento(v))