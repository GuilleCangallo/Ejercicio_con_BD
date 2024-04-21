"""
    Test unitarios para cada función del main

    Prof. Tute Ávalos
"""
# pylint: disable=missing-function-docstring
import pytest
import main
from constantes import DB_TABLES

def test_abrir_conexion_existe():
    main_attrs = dir(main)
    assert 'abrir_conexion' in main_attrs

def test_consulta_generica():
    main_attrs = dir(main)
    assert 'consulta_generica' in main_attrs

@pytest.fixture
def conn_fixture():
    pytest.dbconn = main.abrir_conexion()

# El fixture se puede declarar pasando por parametro una variable
# con el mismo nombre del fixture ->
def test_abrir_conexion(conn_fixture):
    conn = pytest.dbconn
    assert conn

# O se puede declarar con el decorador @pytest.mark.usefixtures(<fixture>)
@pytest.mark.usefixtures("conn_fixture")
@pytest.mark.parametrize('tabla', DB_TABLES)
def test_chequear_tabla(tabla):
    resultado = main.consulta_generica(pytest.dbconn, f"SHOW TABLES LIKE '{tabla}'")
    assert len(resultado) == 1

def pytest_sessionfinish(session, status):
    # pylint: disable=unused-argument
    pytest.dbconn.close()

@pytest.mark.usefixtures("conn_fixture")
def test_listar_clientes_vendor():
    clientes = main.clientes_vendedor(pytest.dbconn, 2)
    assert clientes[0][0] == 1