"""
    Ejemplo de TDD con conexión a base de datos

    Prof. Tute Ávalos
"""
from typing import List
import mysql.connector as db
#import constantes
from constantes import DB_HOSTNAME,DB_DATABASE,DB_PASSWORD,DB_USERNAME


def abrir_conexion() -> db.pooling.PooledMySQLConnection | db.MySQLConnection:
    """Abre una conexión con la base de datos

        Deben especificarse los datos en el módulo de constantes para poder
        conectar a la base de datos local correspondiente.

    Returns:
        PooledMySQLConnection | MySQLConnection: conexión a la base de datos
    """
    return db.connect(host=DB_HOSTNAME,
                      user=DB_USERNAME,
                      password=DB_PASSWORD,
                      database=DB_DATABASE)

def consulta_generica(conn : db.MySQLConnection, consulta : str) -> List[db.connection.RowType]:
    """Hace una consulta la base de datos

    Args:
        conn (MySQLConnection): Conexión a la base de datos obtenida por abrir_conexion()
        consulta (str): Consulta en SQL para hacer en la BD

    Returns:
        List[RowType]: Una lista de tuplas donde cada tupla es un registro y 
                        cada elemento de la tupla es un campo del registro.
    """
    cursor = conn.cursor(buffered=True)
    cursor.execute(consulta)
    return cursor.fetchall()

def clientes_vendedor(conn, id_vendedor) -> List[db.connection.RowType]:
    nv = consulta_generica(conn, f'SELECT count(id) FROM vendedores WHERE id={id_vendedor}')
    if nv[0][0] == 0:
        raise ValueError(f'No existe el vendedor {id_vendedor}')
    return consulta_generica(conn, f'SELECT * FROM clientes WHERE id_vendedor={id_vendedor}')

def listar_todos_vendedores(conn):
    vendedores = consulta_generica(conn, 'SELECT * FROM vendedores')
    return vendedores

def listar_todos_clientes_con_vendedor(conn):
    consulta = '''
        SELECT clientes.id, clientes.nombre, vendedores.nombre 
        FROM clientes 
        INNER JOIN vendedores ON clientes.id_vendedor = vendedores.id
    '''
    clientes = consulta_generica(conn, consulta)
    return clientes

def listar_ordenes_de_vendedor(conn, id_vendedor):
    consulta = f'''
        SELECT ordenes.id, clientes.nombre, vendedores.nombre, ordenes.valor, vendedores.comision 
        FROM ordenes 
        INNER JOIN clientes ON ordenes.cliente_id = clientes.id
        INNER JOIN vendedores ON ordenes.vendedor_id = vendedores.id
        WHERE ordenes.vendedor_id = {id_vendedor}
    '''
    ordenes = consulta_generica(conn, consulta)
    return ordenes

def crear_orden_compra(conn, vendedor_id, cliente_id, fecha, valor, descripcion):
    cursor = conn.cursor(dictionary=True)
    
    consulta_cliente = f"SELECT clientes.sector FROM ventas.clientes WHERE clientes.id = {cliente_id}"
    sector_cliente = consulta_generica(conn, consulta_cliente)
    consulta_vendedor = f"SELECT vendedores.sector FROM ventas.vendedores WHERE vendedores.id = {vendedor_id}"
    sector_vendedor = consulta_generica(conn, consulta_vendedor)
    
    if sector_cliente['sector'] != sector_vendedor['sector']:
        raise ValueError("Error, no coinciden los sectores.")
    if valor < 0:
        raise ValueError("Error, no puede ser negativo.")
    
    cursor.execute(f'''
        INSERT INTO ordenes (vendedor_id, cliente_id, valor, fecha, descripcion)
        VALUES ({vendedor_id}, {cliente_id}, {valor}, {fecha}, "{descripcion}")
    ''')
    
    conn.commit()
    cursor.close()
    
def dar_alta_vendedor(conn, nombre, sector, comision, telefono):
    cursor = conn.cursor()
    cursor.execute(f'''
        INSERT INTO vendedores (nombre, sector, comision, telefono)
        VALUES ("{nombre}", "{sector}", {comision}, {telefono})
    ''')
    conn.commit()
    cursor.close()

if __name__ == "__main__":
    dbconn = abrir_conexion()
    dbconn.close()
