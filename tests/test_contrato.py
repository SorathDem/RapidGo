import jsonschema
import pytest

from tests.helpers import cargar_sample, validar


@pytest.mark.parametrize("nombre", ["evento_valido", "evento_con_falla"])
def test_samples_validos(nombre):
    validar(cargar_sample(nombre))


def test_sample_invalido_es_rechazado():
    with pytest.raises(jsonschema.ValidationError):
        validar(cargar_sample("evento_invalido"))


def test_vehicle_id_con_formato_incorrecto():
    e = cargar_sample("evento_valido")
    e["vehicle_id"] = "VEH-7"
    with pytest.raises(jsonschema.ValidationError):
        validar(e)


def test_temperatura_fuera_de_rango():
    e = cargar_sample("evento_valido")
    e["engine_temp_c"] = 500
    with pytest.raises(jsonschema.ValidationError):
        validar(e)