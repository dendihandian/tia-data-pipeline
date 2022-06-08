import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    "owner": "Airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "youremail@host.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def save_latest_posts_to_json():
    from requests import get
    json_fname = datetime.now().strftime("%Y-%m-%d-%H") + ".json"
    with open(f'/opt/airflow/json/posts/{json_fname}', 'w') as outfile:
        r = get('https://www.techinasia.com/wp-json/techinasia/2.0/posts?page=1&per_page=30', headers={'Content-Type': 'application/json', 'User-Agent': 'Airflow'})
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
        data[index]['vsitems'] = json.dumps(post['vsitems'])
        data[index]['live_items'] = json.dumps(post['live_items'])
        data[index]['author'] = json.dumps(post['author'])
        data[index]['comments'] = json.dumps(post['comments'])
        data[index]['featured_image'] = json.dumps(post['featured_image'])
        data[index]['post_images'] = json.dumps(post['post_images'])
        data[index]['seo'] = json.dumps(post['seo'])
        data[index]['categories'] = json.dumps(post['categories'])
        data[index]['tags'] = json.dumps(post['tags'])
        data[index]['companies'] = json.dumps(post['companies'])
        data[index]['sponsor'] = json.dumps(post['sponsor'])
        data[index]['external_scripts'] = json.dumps(post['external_scripts'])

    with open(f'/opt/airflow/json/posts/{json_fname}', 'w') as outfile:
        outfile.write(json.dumps(data))

def load_posts_data_to_postgres():
    import pandas as pd
    from sqlalchemy import create_engine

    json_fname = datetime.now().strftime("%Y-%m-%d-%H") + ".json"
    posts = pd.read_json(f"/opt/airflow/json/posts/{json_fname}")

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow_db')
    posts.to_sql('posts', con=engine, if_exists='append', index=False)


with DAG(dag_id="tia_pipeline", schedule_interval="*/2 * * * *", default_args=default_args, catchup=False) as dag:

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

    # remove duplication using postgres
    # remove_posts_duplication = PostgresOperator(
    #     task_id="remove_posts_duplication",
    #     sql="""
    #         WITH cte AS (
    #             SELECT 
    #                 *, 
    #                 ROW_NUMBER() OVER (
    #                     PARTITION BY id
    #                 ) row_num
    #             FROM 
    #                 public.posts
    #         )
    #         DELETE FROM cte
    #         WHERE row_num > 1;
    #     """
    # )

    # streams
    is_tia_public_api_accessible >> extract_latest_posts >> transform_posts_data >> load_posts_data
    # load_posts_data >> remove_posts_duplication
