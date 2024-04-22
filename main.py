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

if __name__ == "__main__":
    dbconn = abrir_conexion()

    resultado = consulta_generica(dbconn, "SHOW TABLES")
    for row in resultado:
        print(row[0]) # <- se muestra la primera columna de las filas resultantes

    dbconn.close()
