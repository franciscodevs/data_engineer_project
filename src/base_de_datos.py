from sqlalchemy.engine import Engine
import sqlalchemy as sql
from pandas.core.frame import DataFrame


engine: Engine

def crear_tablas_iniciales() -> None:
    if engine is None:
        engine = __crear_conexion_db()

    # TODO mover esto a Flyway o alguna otra herramienta similar
    with engine.begin() as conn:
        conn.execute(sql.text(
            """
                CREATE TABLE IF NOT EXISTS producto(
                    id bigint GENERATED ALWAYS AS IDENTITY,
                    id_producto bigint UNIQUE,
                    nombre varchar(255),
                --    categoria varchar(255),
                    precio_unitario money,
                    costo_unitario money,
                    costo_reposicion money,
                    margen_ganancia money
                );

                -- ventas
                CREATE TABLE IF NOT EXISTS venta(
                    id bigint GENERATED ALWAYS AS IDENTITY,
                    id_venta bigint,
                    fecha timestamp,
                    id_producto bigint,
                    cantidad int,

                    CONSTRAINT fk_venta_producto FOREIGN KEY (id_producto) REFERENCES producto (id_producto)
                );
            """
        ))

def guardar_dataframe(df: DataFrame, nombre_tabla: str) -> None:
    if engine is None:
        engine = __crear_conexion_db()

    df.to_sql(name=nombre_tabla, con=engine, if_exists="append", index=False, chunksize=10000)

def __crear_conexion_db() -> Engine:
    usuario_db = 'root'
    clave_db = 'root'
    host_db = 'localhost'
    puerto_db = '5432'
    nombre_db = 'ventas'

    url_conexion = f'postgresql://{usuario_db}:{clave_db}@{host_db}:{puerto_db}/{nombre_db}'

    return sql.create_engine(url_conexion)
