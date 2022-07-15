FROM mongo:5.0.9

COPY init_db_collection.js /docker-entrypoint-initdb.d/init_db_collection.js

RUN chmod +x /docker-entrypoint-initdb.d/init_db_collection.js

CMD ["mongod"]

EXPOSE 27017