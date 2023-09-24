import psycopg2
import pandas as pd
import sqlalchemy as sa
from configparser import ConfigParser
import os

# Función para obtención de datos desde archivo csv
def get_data(path):
  try:
    with open(path,"r",encoding="utf-8") as archivo:
    #Leo el archivo 
      df = pd.read_csv(archivo)
    print(f"Tabla {path} cargada correctamente")
  except Exception as e:
    print("Error", e)
  return df

# función de guardado como csv
def save_data_csv(df,path,name):
  try:
    df.to_csv(f"{path}/{name}")
    print(f"Tabla {name} guardada")
  except Exception as e:
    print("Error",e)

# conección con SQLAlchemy
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

# Conección con psycopg2
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

# función para ejecutar un query
def query(cursor, query):
  cursor.execute(query)
  rs = cursor.fetchall()
  # Printeamos los resultados
  for r in rs:
    print(r)

# Función principal
def etl():
    
  # Obtenemos los datos
  df = get_data("data/raw/sales_data.csv")

  # Arreglo columna Product_ean
  df['Product_ean'] = df['Product_ean'].astype(str).apply(lambda x: x.replace('.0',''))

  # Creamos tabla Productos 
  df_prod = df[["Product"]].drop_duplicates()
  df_prod.reset_index(drop=True,inplace=True)
  df_prod["Product_id"] = df_prod.index

  # Intercambiamos columna productos por Product_id
  df = pd.merge(
      df, df_prod,
      left_on="Product", right_on="Product",
      how="inner"
  ).drop("Product",axis=1)

  # Creamos columna Google_Maps
  df["Google_Maps"] = "https://www.google.com/maps/place/" + df['Purchase Address'].apply(lambda / x:x.replace(' ', '+')) 

  """
  En este apartado guardamos las tablas como archivos csv en nuestro 'datalake'
  """
  save_data_csv(df,"data/processed","sales_data_clean.csv")
  save_data_csv(df_prod,"data/processed","products_clean.csv")
  
  
  # Obtenemos los datos limpios
  df_sales = get_data("data/processed/sales_data_clean.csv")
  df_prod = get_data("data/processed/products_clean.csv")
  
  # Creamos la conexión con SQLAlchemy
  try:
    engine = connect_sqlalchemy("config.ini","postgres")
  except Exception as e:
    raise e
  
  # Cargamos la tabla en la base de datos
  df_sales.to_sql("sales_order",engine,if_exists='replace', index=False)
  df_prod.to_sql("products",engine,if_exists='replace', index=False)
  """
  # Query de productos
  cur = connect_psycopg2()
  print("PRODUCTOS: ")
  query(cur, "Select * from products limit 5;")
  """
if __name__ == "__main__":
    etl()