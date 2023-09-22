# Usa la imagen oficial de PostgreSQL como base
FROM postgres:13

# Define variables de entorno para configurar la base de datos
ENV POSTGRES_DB=de-prueba
ENV POSTGRES_USER=root
ENV POSTGRES_PASSWORD=root

# Copia scripts SQL personalizados al contenedor (opcional)
#COPY init.sql /docker-entrypoint-initdb.d/
EXPOSE 5432