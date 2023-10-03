import pandas as pd
from base_de_datos import crear_tablas_iniciales
from base_de_datos import guardar_dataframe


crear_tablas_iniciales()

datos_originales = pd.read_csv("../data/processed/sales_data_fixed.csv", sep=",")

# obtengo los datos de productos
productos = datos_originales.groupby(["Product ID"], as_index=False).agg({
    #"Quantity Ordered": "sum",
    "Price Each": "mean",
    "Cost price": "mean",
    "turnover": "mean",
    "margin": "mean"
})
productos = productos.rename(columns = {
    "Product ID": "id_producto",
    "Product": "nombre",
    "Price Each": "precio_unitario",
    "Cost price": "costo_unitario",
    "turnover": "costo_reposicion",
    "margin": "margen_ganancia"
})

guardar_dataframe(productos, "producto")

# obtengo los datos de las ventas
ventas = datos_originales[["Order ID", "Order Date", "Product ID", "Quantity Ordered"]]
ventas = ventas.rename(columns={
    "Order ID": "id_venta",
    "Order Date": "fecha",
    "Product ID": "id_producto",
    "Quantity Ordered": "cantidad"
})

guardar_dataframe(ventas, "venta")
