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

def test_listar_clientes_de_vendedor_existe():
    main_attrs = dir(main)
    assert 'listar_clientes_de_vendedor' in main_attrs

def test_listar_todos_vendedores_existe():
    main_attrs = dir(main)
    assert 'listar_todos_vendedores' in main_attrs
    
def test_listar_todos_clientes_con_vendedor_existe():
    main_attrs = dir(main)
    assert 'listar_todos_clientes_con_vendedor' in main_attrs
    
def test_listar_ordenes_de_vendedor_existe():
    main_attrs = dir(main)
    assert 'listar_ordenes_de_vendedor' in main_attrs

def test_crear_orden_compra_existe():
    main_attrs = dir(main)
    assert 'crear_orden_compra' in main_attrs
    
def test_dar_alta_vendedor_existe():
    main_attrs = dir(main)
    assert 'dar_alta_vendedor' in main_attrs

def test_dar_alta_cliente_existe():
    main_attrs = dir(main)
    assert 'dar_alta_cliente' in main_attrs
    
def test_eliminar_orden_de_compra_existe():
    main_attrs = dir(main)
    assert 'eliminar_orden_de_compra' in main_attrs
    
def test_dar_baja_vendedor_existe():
    main_attrs = dir(main)
    assert 'dar_baja_vendedor' in main_attrs
    
def test_dar_baja_cliente_existe():
    main_attrs = dir(main)
    assert 'dar_baja_cliente' in main_attrs   

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
    
@pytest.mark.usefixtures("conn_fixture")
def test_listar_clientes_vendedor():
    clientes = main.clientes_vendedor(pytest.dbconn, 2)
    assert clientes[0][0] == 1

@pytest.mark.usefixtures("conn_fixture")
def test_listar_todos_vendedores():
    vendedores = main.listar_todos_vendedores(pytest.dbconn)
    assert vendedores != 0
    
@pytest.mark.usefixtures("conn_fixture")
def test_listar_todos_clientes_con_vendedor():
    clientes = main.listar_todos_clientes_con_vendedor(pytest.dbconn)
    assert clientes != 0
    
@pytest.mark.usefixtures("conn_fixture")
def test_listar_ordenes_de_vendedor():
    id_vendedor = 1
    ordenes_de_vendedor = main.listar_ordenes_de_vendedor(pytest.dbconn, id_vendedor)
    assert ordenes_de_vendedor != 0
