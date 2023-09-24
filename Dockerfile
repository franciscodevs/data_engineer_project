# Utiliza una imagen de Python 3.11
FROM python:3.11

# Copia el archivo requirements.txt al directorio /app en el contenedor
COPY requirements.txt /app/requirements.txt
COPY data /app/data
COPY config.ini /app/config.ini

# Establece el directorio de trabajo
WORKDIR /app

# Crea y activa un entorno virtual de Python
RUN python -m venv env
SHELL ["/bin/bash", "-c"]
RUN source env/bin/activate

# Instala las dependencias desde el archivo requirements.txt
RUN pip install -r requirements.txt

# Copia otro archivo Python a /app
COPY etl.py /app/etl.py

# Establece una variable de entorno con el nombre del servicio de la base de datos
ENV DB_HOST=data_engineer_project-database-1

# Ejecuta el otro script después de la instalación de las dependencias
CMD ["python", "etl.py"]