FROM python:3.11

RUN python -m venv "var/lib"
RUN python -m pip install --upgrade pip
RUN pip install pandas SQLAlchemy psycopg2

COPY etl.py var/lib

ENTRYPOINT [ "python", "etl.py" ]