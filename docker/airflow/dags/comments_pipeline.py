import json
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.sql import SqlSensor
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

def extract_yesterday_post_comments():
    from sqlalchemy import create_engine
    from sqlalchemy.sql import text
    from requests import get

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    today = today.strftime("%Y-%m-%d")
    yesterday = yesterday.strftime("%Y-%m-%d")

    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow_db')
    result = engine.execute(text("SELECT id AS post_id FROM public.posts WHERE date_gmt >= :yesterday AND date_gmt < :today"), {'today': today, 'yesterday': yesterday}).fetchall()
    post_ids = [row['post_id'] for row in result]
    comments_json_files = []

    for post_id in post_ids:
        r = get(f'https://www.techinasia.com/wp-json/techinasia/2.0/posts/{post_id}/comments', headers={'Content-Type': 'application/json', 'User-Agent': 'Airflow'})
        comments = r.json()['comments']

        if comments:
            with open(f'/opt/airflow/json/comments/{yesterday}_{post_id}.json', 'w') as outfile:
                outfile.write(json.dumps(comments))
                logging.info(f'{post_id} comments extracted')
            comments_json_files.append(post_id)

    logging.info(f'{len(comments_json_files)} comments files created')
    with open(f'/opt/airflow/json/comments/{yesterday}.json', 'w') as outfile:
        outfile.write(json.dumps(comments_json_files))


def transform_comments_json():

    yesterday = datetime.now() - timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")

    posts_ids = []
    with open(f'/opt/airflow/json/comments/{yesterday}.json', 'r') as file_content:
        posts_ids = json.load(file_content)

    for post_id in posts_ids:
        comments = []
        with open(f'/opt/airflow/json/comments/{yesterday}_{post_id}.json', 'r') as file_content:
            comments = json.load(file_content)

        if comments:
            for index, comment in enumerate(comments):
                for key in list(comment.keys()):
                    if type(comment[key]) in [dict, list]:
                        comments[index][key] = json.dumps(comment[key])
                    elif type(comment[key]) == str and len(comment[key]) == 19 and comment[key][10] == "T":
                        comments[index][key] = comment[key].replace('T', ' ')

                comments[index]['word_count'] = len(comment['content'].split())

            with open(f'/opt/airflow/json/comments/{yesterday}_{post_id}.json', 'w') as outfile:
                outfile.write(json.dumps(comments))
                logging.info(f'{post_id} comments transformed')

def load_comments_data_to_postgres():
    from sqlalchemy import create_engine
    from sqlalchemy.sql import text

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow_db')
    yesterday = datetime.now() - timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")

    posts_ids = []
    with open(f'/opt/airflow/json/comments/{yesterday}.json', 'r') as file_content:
        posts_ids = json.load(file_content)

    for post_id in posts_ids:
        comments = []
        with open(f'/opt/airflow/json/comments/{yesterday}_{post_id}.json', 'r') as file_content:
            comments = json.load(file_content)

        if type(comments) == list and len(comments) > 0:
            for comment in comments:
                sql = text(f"""
                    INSERT INTO public.comments (id,post,post_title,post_link,post_featured_image,post_comments_count,content,excerpt,status,type,parent,author,date_gmt,can_edit,editable_until,children,word_count)
                    VALUES (:id,:post,:post_title,:post_link,:post_featured_image,:post_comments_count,:content,:excerpt,:status,:type,:parent,:author,:date_gmt,:can_edit,:editable_until,:children,:word_count)
                    ON CONFLICT ON CONSTRAINT comments_pkey DO NOTHING;
                """)
                engine.execute(sql, {'id': comment['id'],'post': comment['post'],'post_title': comment['post_title'],'post_link': comment['post_link'],'post_featured_image': comment['post_featured_image'],'post_comments_count': comment['post_comments_count'],'content': comment['content'],'excerpt': comment['excerpt'],'status': comment['status'],'type': comment['type'],'parent': comment['parent'],'author': comment['author'],'date_gmt': comment['date_gmt'],'can_edit': comment['can_edit'],'editable_until': comment['editable_until'],'children': comment['children'],'word_count': comment['word_count']})


with DAG(dag_id="comments_pipeline", schedule_interval="0 1 * * *", default_args=default_args, catchup=False) as dag:

    # check if the tia public api is accessible
    is_tia_public_api_accessible = HttpSensor(
        task_id="is_tia_public_api_accessible",
        http_conn_id="tia_api",
        method="GET",
        endpoint="posts",
        headers={'Content-Type': 'application/json', 'User-Agent': 'Airflow'},
        response_check=lambda response: "posts" in response.text
    )

    # check postgres connection
    is_postgres_accessible = SqlSensor(
        task_id="is_postgres_accessible",
        conn_id="tia_postgres",
        sql="SELECT 1 AS is_accessible;",
        success=lambda row: row > 0
    )

    # extract comments of yesterday posts
    extract_comments = PythonOperator(
        task_id="extract_comments",
        python_callable=extract_yesterday_post_comments
    )

    # transform_comments 
    transform_comments = PythonOperator(
        task_id="transform_comments",
        python_callable=transform_comments_json
    )

    # load comments into postgres
    load_comments = PythonOperator(
        task_id="load_comments",
        python_callable=load_comments_data_to_postgres
    )

    # remove comments json files
    remove_comments_json = BashOperator(
        task_id="remove_comments_json",
        bash_command=f"""
            rm -rf /opt/airflow/json/comments/{(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")}*
        """
    )

    # streams
    is_tia_public_api_accessible >> is_postgres_accessible >> extract_comments >> transform_comments >> load_comments >> remove_comments_json
