import pandas as pd
# from requests.exceptions import RequestException

def get_data(path):
  try:
    with open(path,"r",encoding="utf-8") as archivo:
    #Leo el archivo 
      df = pd.read_csv(archivo)
    print(f"Tabla {path} cargada correctamente")
  except Exception as e:
    print("Error", e)
  return df

# función de guardado
def save_data_csv(df,path,name):
  try:
    df.to_csv(f"{path}/{name}")
    print(f"Tabla {name} guardada")
  except Exception as e:
    print("Error",e)


def etl():
    

  df = get_data("data/raw/sales_data.csv")
  # get_data("data/raw/sales_data.csv")

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
  df["Google_Maps"] = "https://www.google.com/maps/place/" + df['Purchase Address'].apply(lambda x:x.replace(' ', '+')) 

  #guardamos
  save_data_csv(df,"data/processed","sales_data_clean.csv")
  save_data_csv(df_prod,"data/processed","products_clean.csv")
  
  
if __name__ == "__main__":
    etl()

