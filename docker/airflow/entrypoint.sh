#!/usr/bin/env bash

# Initiliase the metastore
airflow db init

# Run the scheduler in background
airflow scheduler &> /dev/null &

# Create user
airflow users create -u admin -p admin -r Admin -e admin@admin.com -f admin -l admin

# Create custom connection for dags and tasks
airflow connections add tia_api --conn-type http --conn-host https://www.techinasia.com/wp-json/techinasia/2.0/
airflow connections add tia_postgres --conn-type postgres --conn-host postgres --conn-port 5432 --conn-schema tia_db --conn-login airflow --conn-password airflow

# Run the web server in foreground (for docker logs)
exec airflow webserver
