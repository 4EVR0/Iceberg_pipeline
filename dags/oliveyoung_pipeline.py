from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

IMAGE = "oliveyoung-pipeline:latest"

with DAG(
    dag_id="oliveyoung_bronze_to_silver",
    schedule=None,  # 크롤링 DAG의 TriggerDagRunOperator로 실행
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["oliveyoung", "etl"],
) as dag:

    sync_reference = DockerOperator(
        task_id="sync_reference_data",
        image=IMAGE,
        command="sync_reference",
        network_mode="host",
        auto_remove="success",
        environment={"AWS_DEFAULT_REGION": "ap-northeast-2"},
    )

    bronze_to_silver = DockerOperator(
        task_id="bronze_to_silver",
        image=IMAGE,
        command="bronze_to_silver",
        network_mode="host",
        auto_remove="success",
        environment={"AWS_DEFAULT_REGION": "ap-northeast-2"},
    )

    sync_reference >> bronze_to_silver
