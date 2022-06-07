import airflow
import requests
import logging
import json
from airflow import settings
from airflow import DAG
# from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models.connection import Connection
from datetime import datetime, timedelta

default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "youremail@host.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def create_tia_api_conn():
    conn = Connection(
        conn_id='tia_api', 
        conn_type='http', 
        host='https://www.techinasia.com/wp-json/techinasia/2.0/'
    )
    session = settings.Session()
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if str(conn_name) == str(conn.conn_id):
        logging.warning(f"Connection {conn.conn_id} already exists")
        return True

    session.add(conn)
    session.commit()
    logging.info(Connection.log_info(conn))
    logging.info(f'Connection {conn.conn_id} is created')

    return True

def save_latest_posts_to_json():
    json_fname = datetime.now().strftime("%Y-%m-%d-%H") + ".json"
    with open(f'/opt/airflow/json/posts/{json_fname}', 'w') as outfile:
        r = requests.get('https://www.techinasia.com/wp-json/techinasia/2.0/posts?page=1&perpage=50', headers={'Content-Type': 'application/json', 'User-Agent': 'Airflow'})
        outfile.write(json.dumps(r.json()['posts']))


with DAG(dag_id="tia_pipeline", schedule_interval="@hourly", default_args=default_args, catchup=False) as dag:

    # create and enable connection
    create_tia_api_connection = PythonOperator(
        task_id="create_tia_api_connection",
        python_callable=create_tia_api_conn
    )

    # check if the tia public api is accessible
    is_tia_public_api_accessible = HttpSensor(
        task_id="is_tia_public_api_accessible",
        http_conn_id="tia_api",
        method="GET",
        endpoint="posts",
        headers={'Content-Type': 'application/json', 'User-Agent': 'Airflow'},
        response_check=lambda response: "posts" in response.text
    )

    # load last hour of posts from tia public api
    extract_latest_posts = PythonOperator(
        task_id="extract_latest_posts",
        python_callable=save_latest_posts_to_json
    )

    # streams
    create_tia_api_connection >> is_tia_public_api_accessible >> extract_latest_posts
