import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2


def extract(path):
    """
    Extraer la tabla de ventas a un dataframe

    Args:
        Path: Ubicacion del archivo en formato CSV

    Return:
        pd.DataFrame: DataFrame con la informacion de órdenes de venta
    """
    df = pd.read_csv(path, encoding='utf-8')
    
    return df

def transform(df):
    """
    1. Crear tabla de productos
    2. Crear tabla de direcciones de compra con sus respectivas coordenadas

    Args:
        df (pd.DataFrame): DataFame con las órdenes de venta

    Return: 
        1. df_prod (pd.DataFrame): Tabla con los productos y sus respectivos ID
        2. df (pd.DataFrame): Tabla con la columna de productos relacionada 
        con la tabla de productos
        3. df_adresses (pd.DataFrame): Tabla con las direcciones geocodificadas
    """
    # Tabla de productos
    df_prod = df['Product'].unique().tolist()
    df_prod = pd.DataFrame(df_prod, columns=['Product']).rename_axis('product_id').reset_index()

    # Relacionar tabla de ventas con el id de tabla productos
    df = pd.merge(
      df, df_prod,
      left_on="Product", right_on="Product",
      how="inner").drop("Product",axis=1)

    # Crear la tabla con las direcciones geocodificadas
    df_adresses = df[['Order ID','Purchase Address']].drop_duplicates().reset_index(drop=True)
    df_adresses["Google_Maps"] = "https://www.google.com/maps/place/" + df_adresses['Purchase Address'].apply(lambda x:x.replace(' ', '+'))

    return df_prod, df, df_adresses

def load(df_prod, df, df_adresses):
    """
    1. Crear la conexión a la base de datos
    2. Crear la tabla de base de datos con los DataFrame
    3. Cargar la base de datos con los registros

    Args:
        df_prod (pd.DataFrame): Tabla con los productos y sus ID
        df (pd.DataFrame): Tabla con la información de ventas
        df_adresses (pd.DataFrame): Tabla con la ubicacion de las direcciones

    Return: 
        None
    """

    engine = create_engine(f'postgresql://postgres:postgres@localhost:5432/sales')

    df_prod.head(n=0).to_sql(name='products', con=engine, if_exists='replace', index=False)
    df_prod.to_sql(name='products', con=engine, if_exists='append', index=False)

    df.head(n=0).to_sql(name='sales_order', con=engine, if_exists='replace', index=False)
    df.to_sql(name='sales_order', con=engine, if_exists='append', index=False)

    df_adresses.head(n=0).to_sql(name='adresses', con=engine, if_exists='replace', index=False)
    df_adresses.to_sql(name='adresses', con=engine, if_exists='append', index=False)

def etl():
    path = 'data/sales_data.csv'

    df = extract(path)

    df_prod, df, df_adresses = transform(df)

    load(df_prod, df, df_adresses)

if __name__ == '__main__':
    etl()
