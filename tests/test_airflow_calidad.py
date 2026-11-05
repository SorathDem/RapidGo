"""P3: las validaciones de calidad fallan cuando deben (fallo controlado)."""
import pytest

from airflow.dags.rg_calidad import CalidadError, validar_processed, validar_raw
from tests.helpers_airflow import escribir_processed, escribir_raw, fila


def test_validar_raw_falla_si_esta_vacio(tmp_path):
    with pytest.raises(CalidadError, match="RAW vacio"):
        validar_raw(tmp_path)
    (tmp_path / "raw" / "telemetry").mkdir(parents=True)  # carpeta existe pero sin parquet
    with pytest.raises(CalidadError, match="RAW vacio"):
        validar_raw(tmp_path)


def test_validar_raw_ok_y_processed_detecta_esquema_invalido(tmp_path):
    assert validar_raw(escribir_raw(tmp_path, n=3)) == {"filas_raw": 3}

    f = fila()
    del f["temp_critica"]  # esquema invalido: falta una columna requerida
    escribir_processed(tmp_path, [f])
    with pytest.raises(CalidadError, match="Esquema invalido"):
        validar_processed(tmp_path)