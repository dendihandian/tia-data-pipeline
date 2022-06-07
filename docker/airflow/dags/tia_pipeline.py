import airflow
import requests
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.models.connection import Connection
from datetime import datetime, timedelta
from sqlalchemy import create_engine

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

def save_latest_posts_to_json():
    json_fname = datetime.now().strftime("%Y-%m-%d-%H") + ".json"
    with open(f'/opt/airflow/json/posts/{json_fname}', 'w') as outfile:
        r = requests.get('https://www.techinasia.com/wp-json/techinasia/2.0/posts?page=1&per_page=30', headers={'Content-Type': 'application/json', 'User-Agent': 'Airflow'})
        outfile.write(json.dumps(r.json()['posts']))

def transform_posts_json():
    json_fname = datetime.now().strftime("%Y-%m-%d-%H") + ".json"

    data = None
    with open(f'/opt/airflow/json/posts/{json_fname}', 'r') as file_content:
        data = json.load(file_content)

    for index, post in enumerate(data):
        data[index]['date_gmt'] = post['date_gmt'].replace('T', ' ').replace('Z', '')
        data[index]['modified_gmt'] = post['modified_gmt'].replace('T', ' ').replace('Z', '')
        data[index]['word_count'] = len(post['content'].split())
        data[index]['vsitems'] = str(post['vsitems'])
        data[index]['live_items'] = str(post['live_items'])
        data[index]['author'] = str(post['author'])
        data[index]['comments'] = str(post['comments'])
        data[index]['featured_image'] = str(post['featured_image'])
        data[index]['post_images'] = str(post['post_images'])
        data[index]['seo'] = str(post['seo'])
        data[index]['categories'] = str(post['categories'])
        data[index]['tags'] = str(post['tags'])
        data[index]['companies'] = str(post['companies'])
        data[index]['sponsor'] = str(post['sponsor'])
        data[index]['external_scripts'] = str(post['external_scripts'])

    with open(f'/opt/airflow/json/posts/{json_fname}', 'w') as outfile:
        outfile.write(json.dumps(data))

def load_posts_data_to_postgres():
    json_fname = datetime.now().strftime("%Y-%m-%d-%H") + ".json"
    posts = pd.read_json(f"/opt/airflow/json/posts/2022-06-07-19.json")

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow_db')
    posts.head(1).to_sql('posts', con=engine, if_exists='append', index=False)


with DAG(dag_id="tia_pipeline", schedule_interval="@hourly", default_args=default_args, catchup=False) as dag:

    # check if the tia public api is accessible
    is_tia_public_api_accessible = HttpSensor(
        task_id="is_tia_public_api_accessible",
        http_conn_id="tia_api",
        method="GET",
        endpoint="posts",
        headers={'Content-Type': 'application/json', 'User-Agent': 'Airflow'},
        response_check=lambda response: "posts" in response.text
    )

    # extract latest posts from tia public api
    extract_latest_posts = PythonOperator(
        task_id="extract_latest_posts",
        python_callable=save_latest_posts_to_json
    )

    # transform posts data
    transform_posts_data = PythonOperator(
        task_id="transform_posts_data",
        python_callable=transform_posts_json
    )

    # load posts json to postgres
    load_posts_data = PythonOperator(
        task_id="load_posts_data",
        python_callable=load_posts_data_to_postgres
    )

    # streams
    is_tia_public_api_accessible >> extract_latest_posts >> transform_posts_data
