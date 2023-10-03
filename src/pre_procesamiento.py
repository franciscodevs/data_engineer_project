import pandas as pd
import numpy as np

# leo el csv
ventas = pd.read_csv("../data/raw/sales_data.csv", sep=",")

# obtengo un id de producto del CSV
productos = ventas["Product"].drop_duplicates().reset_index()
productos.index = np.arange(1, len(productos) + 1)
productos["Product ID"] = productos.index

# mergeo los dos dataframes para tener el id de producto
df_final = pd.merge(
    ventas, productos,
    left_on="Product", right_on="Product",
    how="left"
).drop("Product", axis=1)

# reordeno las columnas
df_final = df_final.reindex(columns=['Order Date', 'Order ID', 'Product', 'Product ID', 'Product_ean', 'catégorie',
                                     'Purchase Address', 'Quantity Ordered', 'Price Each', 'Cost price',
                                     'turnover', 'margin'])

df_final.to_csv("../data/processed/sales_data_fixed.csv", sep=",", index=False)
