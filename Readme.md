# Proyecto ETL

## Objetivos
- Crear un entorno virtual para instalar las librerias necesarias de Python
  
- Usar el dataset [Sales Order](https://www.kaggle.com/datasets/vincentcornlius/sales-orders) de Kaggle 


- Realizar un analisis y transformacion de los datos a fin de crear 2 tablas:
  - Tabla de productos con su ID
  - Tabla de direcciones de compra con sus respectivas coordenadas<p></p>

- Crear una base de datos PostgreSQL en Docker

- Guardar las tablas en la base de datos

## Ejecución
Primero iniciamos la base de datos con Docker Compose:

```
docker-compose up
```

Para construir la imagen del ETL:

```
docker build -t imagen-python-primer-etl .
```

Para ejecutar la imagen:

```
docker run --name container-python-primer-etl -p 8080:8080 -d imagen-python-primer-etl
```
