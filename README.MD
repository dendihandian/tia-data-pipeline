# TIA ETL

ETL data pipeline project to ingest data from TIA Public API.

## DAGS
- `posts_pipeline`
    - runs hourly to ingest 30 latest posts
- `comments_pipeline`
    - runs daily at 1AM to ingest yesterday posts comments

## Development Setup

### Requirements
- Docker and Docker-Compose
    - have 2GB RAM allocated for the VM and 2GB free disk space to run the services.

### Setting Up The Project
1. Clone or download this repository to your machine.
2. Open your terminal/console and make sure `docker` and `docker-compose` commands are executable.
3. Go to this repository root directory in using your terminal.
4. Execute `docker-compose build` to build airflow, postgres, adminer as docker services.

    > If there is any error, try to execute command several times until it succeed. if it's no luck, raise an issue.

5. After all the service built, execute `docker-compose up -d` to start the services.
    > You can inspect the running services by executing `docker-compose ps` to see if the services are up or failing. if one of the service is failing, you may want to inspect it using `docker-compose logs <service_name>` (eg. `docker-compose logs airflow`). another issue that may happen are the existing services with the same port used by our services. if that happens, try to stop the existing service(s) first and then start our project services.
6. After all the service running, you can start to open the Airflow UI at [http://localhost:8080](http://localhost:8080) to make sure the service really went up.

### Start/Pause DAGs
1. Open Airflow UI at [http://localhost:8080](http://localhost:8080).
2. Login using `admin` as username and `admin` as password.
3. You will redirected to home page, you can find our paused DAGs here. Let's start with the `posts_pipeline` DAG. 

![posts_pipeline_inlist](/docs/images/posts_pipeline_inlist.png)

4. Click on the `posts_pipeline` title to enter the DAG page or open [http://localhost:8080/tree?dag_id=posts_pipeline](http://localhost:8080/tree?dag_id=posts_pipeline).
5. On the post pipeline DAG page, you can click the blue switch to start the DAG.

   > another thing you may want to check in this page is to see the `Tree View` and `Graph View` to check if the tasks are running, retrying or failed. you may want to check the DAG code by clicking the `Code` tab. 


![posts_pipeline_on](/docs/images/posts_pipeline_on.png)

6. After all inspection done, you can pause the DAG by switch it back to off or leave it run on schedule.

7. You can do the same for the `comments_pipeline`

![comments_pipeline_inlist](/docs/images/comments_pipeline_inlist.png)

### Monitoring The Data Using Adminer

1. Open Adminer web UI at [localhost:32767](http://localhost:32767).
2. Login using `airflow` as username and `airflow` as password. make sure use `PostgreSQL` as system, `postgres` as system, and `airflow_db` as database.

![adminer_login_page](/docs/images/adminer_login_page.png)

3. After logged in, you can start to monitor the posts data ingested by choosing DB: `airflow_db`, Schema: `public` and click on the `posts` table.

![posts_monitoring](/docs/images/posts_monitoring.png)

### Modify DAGs

You can start to edit any availabe dags under `docker/airflow/dags` folder by using any code editor you prefered. the Docker volume system will map the change to the associated dags in the container.

> You may want to change any DAG schedule by changing the cron expression in the dag `schedule_interval` parameter.

After you modified the dags file, you can inspect it in the edited dag page on Airflow UI and see the changes applied on `Code` tab. 

### Test DAGs

You can test DAGs pipeline using the command:

```
docker-compose exec airflow airflow dags test <dag_id> <desired_execution_date>
```

Here is the example to test the `posts_pipeline` dag:

```
docker-compose exec airflow airflow dags test comments_pipeline 2022-01-01
```

and here is the example to test the `comments_pipeline` dag:

```
docker-compose exec airflow airflow dags test comments_pipeline 2022-01-01T01:00:00
```

### Test DAGs Task

When you are developing a task, you may want to test it individually using the command:

```
docker-compose exec airflow airflow tasks test <dag_id> <task_id> <desired_execution_date>
```

Here is the example of testing one of `posts_pipeline` task:

```
docker-compose exec airflow airflow tasks test posts_pipeline is_tia_public_api_accessible 2022-01-01
```

here is the example of testing one of `comments_pipeline` task:

```
docker-compose exec airflow airflow tasks test comments_pipeline is_postgres_accessible 2022-01-01T01:00:00 
```