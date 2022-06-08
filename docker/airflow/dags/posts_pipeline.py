import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.sql import SqlSensor
from datetime import timedelta
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

post_json_fname = "temp_posts.json"

def save_latest_posts_to_json():
    from requests import get
    with open(f'/opt/airflow/json/posts/{post_json_fname}', 'w') as outfile:
        r = get('https://www.techinasia.com/wp-json/techinasia/2.0/posts?page=1&per_page=30', headers={'Content-Type': 'application/json', 'User-Agent': 'Airflow'})
        outfile.write(json.dumps(r.json()['posts']))

def transform_posts_json():
    posts = []
    with open(f'/opt/airflow/json/posts/{post_json_fname}', 'r') as file_content:
        posts = json.load(file_content)

    for index, post in enumerate(posts):
        for key in list(post.keys()):
            if type(post[key]) in [dict, list]:
                posts[index][key] = json.dumps(post[key])
            elif type(post[key]) == str and len(post[key]) == 19 and post[key][10] == "T":
                posts[index][key] = post[key].replace('T', ' ')

        posts[index]['word_count'] = len(post['content'].split())

    with open(f'/opt/airflow/json/posts/{post_json_fname}', 'w') as outfile:
        outfile.write(json.dumps(posts))

def load_posts_data_to_postgres():
    from sqlalchemy import create_engine
    from sqlalchemy.sql import text
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow_db')

    with open(f"/opt/airflow/json/posts/{post_json_fname}", "r") as f:
        posts = json.load(f)
        for post in posts:
            sql = text(f"""
                INSERT INTO public.posts (id,date_gmt,modified_gmt,title,slug,status,type,link,content,vsitems,live_items,excerpt,author,editor,comment_status,comments_count,comments,featured_image,post_images,seo,categories,tags,companies,is_sponsored,sponsor,is_partnership,external_scripts,show_ads,is_subscriber_exclusive,is_paywalled,is_inappbrowser,read_time,word_count)
                VALUES (:id,:date_gmt,:modified_gmt,:title,:slug,:status,:type,:link,:content,:vsitems,:live_items,:excerpt,:author,:editor,:comment_status,:comments_count,:comments,:featured_image,:post_images,:seo,:categories,:tags,:companies,:is_sponsored,:sponsor,:is_partnership,:external_scripts,:show_ads,:is_subscriber_exclusive,:is_paywalled,:is_inappbrowser,:read_time,:word_count)
                ON CONFLICT ON CONSTRAINT posts_pkey DO NOTHING;
            """)
            engine.execute(sql, {'id': post['id'],'date_gmt': post['date_gmt'],'modified_gmt': post['modified_gmt'],'title': post['title'],'slug': post['slug'],'status': post['status'],'type': post['type'],'link': post['link'],'content': post['content'],'vsitems': post['vsitems'],'live_items': post['live_items'],'excerpt': post['excerpt'],'author': post['author'],'editor': post['editor'],'comment_status': post['comment_status'],'comments_count': post['comments_count'],'comments': post['comments'],'featured_image': post['featured_image'],'post_images': post['post_images'],'seo': post['seo'],'categories': post['categories'],'tags': post['tags'],'companies': post['companies'],'is_sponsored': post['is_sponsored'],'sponsor': post['sponsor'],'is_partnership': post['is_partnership'],'external_scripts': post['external_scripts'],'show_ads': post['show_ads'],'is_subscriber_exclusive': post['is_subscriber_exclusive'],'is_paywalled': post['is_paywalled'],'is_inappbrowser': post['is_inappbrowser'],'read_time': post['read_time'],'word_count': post['word_count']})



with DAG(dag_id="posts_pipeline", schedule_interval="@hourly", default_args=default_args, catchup=False) as dag:

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

    remove_posts_json = BashOperator(
        task_id="remove_posts_json",
        bash_command=f"""
            rm -rf /opt/airflow/json/posts/{post_json_fname}
        """
    )

    # streams
    is_tia_public_api_accessible >> is_postgres_accessible >> extract_latest_posts >> transform_posts_data >> load_posts_data >> remove_posts_json
