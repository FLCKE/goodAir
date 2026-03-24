from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'run_meteo',
    default_args=default_args,
    schedule_interval='@hourly',  # toutes les heures
    catchup=False
)

run_app = DockerOperator(
    task_id='run_meteo_app',
    image='etl-last-app:latest',             # ton container app
    api_version='auto',
    auto_remove=False,
    command='python /app/main.py',     # chemin à l’intérieur du container
    docker_url='unix://var/run/docker.sock',
    network_mode='project_default',    # réseau docker-compose
    mount_tmp_dir=False,
    environment={
        "DB_NAME": "goodair",
        "DB_USER": "admin",
        "DB_PASSWORD": "a remplir",
        "DB_HOST": "postgres",
        "API_OPEN":"a remplir",
        "API_AIR":"a remplir"
    },
    retries=2,
    retry_delay=timedelta(seconds=10),
    dag=dag,
)