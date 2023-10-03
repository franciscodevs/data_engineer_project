FROM python:3.11

RUN python -m venv "var/lib"
RUN python -m pip install --upgrade pip
RUN pip install pandas SQLAlchemy psycopg2

COPY etl.py var/lib

ENTRYPOINT [ "python", "etl.py" ]

# para construir la imagen: docker build -t imagen-python-primer-etl .
# para ejecutar el container: docker run --name container-python-primer-etl -p 8080:8080 -d imagen-python-primer-etl