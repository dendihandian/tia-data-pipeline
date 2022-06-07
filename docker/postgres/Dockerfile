FROM postgres:11.4-alpine

COPY init_db_tables.sh /docker-entrypoint-initdb.d/init_db_tables.sh

RUN chmod +x /docker-entrypoint-initdb.d/init_db_tables.sh

EXPOSE 5432