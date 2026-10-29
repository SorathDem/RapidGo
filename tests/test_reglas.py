from spark.rules import mantenimiento_vencido, temperatura_critica


def test_a1_vencido():
    assert mantenimiento_vencido(5025.7)

def test_a1_en_el_limite_no_dispara():
    assert not mantenimiento_vencido(5000.0)

def test_a2_dos_lecturas_criticas():
    assert temperatura_critica([90, 106, 112])

def test_a2_pico_aislado_no_dispara():
    assert not temperatura_critica([90, 112, 95])

def test_a2_en_el_umbral_no_cuenta():
    assert not temperatura_critica([105.0, 105.0])