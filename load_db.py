import psycopg2
import pandas as pd
import sqlalchemy as sa
from configparser import ConfigParser
import os
from etl import get_data

def connect_sqlalchemy(config_file_path="config.ini", section="postgres"):

  # Comprobar si el archivo de configuración existe
  if not os.path.exists(config_file_path):
    raise FileNotFoundError(f"El archivo de configuración '{config_file_path}' no existe.")

  # Leer la configuración desde el archivo INI
  config = ConfigParser()
  config.read(config_file_path)
  conn_data = config[section]

  # Obtener los parámetros de conexión
  host = conn_data.get("host")
  port = conn_data.get("port")
  database = conn_data.get("database")
  user = conn_data.get("user")
  password = conn_data.get("password")

  url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

  # Establecer la conexión a la base de datos PostgreSQL
  try:
    conn = sa.create_engine(url)

    
  except Exception as e:
    print("Error de conexión", e)
  return conn

def connect_psycopg2(config_file_path="config.ini", section="postgres"):
  # Comprobar si el archivo de configuración existe
  if not os.path.exists(config_file_path):
    raise FileNotFoundError(f"El archivo de configuración '{config_file_path}' no existe.")
  # Leer la configuración desde el archivo INI
  config = ConfigParser()
  config.read(config_file_path)
  conn_data = config[section]

  try:
    conn=psycopg2.connect(
      host=conn_data.get("host"),
      port=conn_data.get("port"),
      user=conn_data.get("user"),
      password=conn_data.get("password"),
      database=conn_data.get("database")
    )
    cur=conn.cursor()
  except Exception as e:
    print("Error en psycopg2", e)
  
  #Retornamos cursor
  return cur


def query(cursor, query):
  cursor.execute(query)
  rs = cursor.fetchall()
  # Printeamos los resultados
  for r in rs:
    print(r)


def load_db():

  # Obtenemos los datos

  df_sales = get_data("data/processed/sales_data_clean.csv")
  df_prod = get_data("data/processed/products_clean.csv")
  
  # Creamos la conexión con SQLAlchemy
  engine = connect_sqlalchemy("config.ini","postgres")

  # Cargamos la tabla en la base de datos
  #df_sales.to_sql("sales_order",engine,if_exists='replace', index=False)
  df_prod.to_sql("products",engine,if_exists='replace', index=False)


  # Creamos cursor con psycopg2
  #cur = connect_psycopg2("config.ini", "postgres")

  # Ejecutamos query
  #print("Top 5 prod: ")    
  #query(cur,"""select * from products limit 5;""")

  #print("Top 5 sales: ")    
  #query(cur,"""select * from sales_order limit 5;""")


if __name__ == "__main__":
    load_db()
