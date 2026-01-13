"""
StreamFlow Analytics Platform - Main Orchestration DAG
Orchestrates: Kafka Ingest -> Spark ETL -> Validation
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os


default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def validate_gold_zone(ds, **_):
    gold_path = f"/opt/spark-data/gold/{ds}"

    if not os.path.exists(gold_path):
        raise ValueError(f"Gold zone path does not exist: {gold_path}")

    files = os.listdir(gold_path)
    if not files:
        raise ValueError("Gold zone exists but is empty")

    print(f"Validation passed — {len(files)} files found in {gold_path}")


with DAG(
    dag_id="streamflow_main",
    default_args=default_args,
    start_date=datetime(2026, 1, 12),
    schedule_interval="@daily",
    catchup=False,
    tags=["streamflow", "etl"],
) as dag:

    ingest_kafka = BashOperator(
        task_id="ingest_kafka_to_landing",
        bash_command="""
        python /opt/spark-jobs/ingest_kafka_to_landing.py \
        --output-path /opt/spark-data/landing/{{ ds }}
        """,
    )

    spark_etl = BashOperator(
        task_id="spark_etl",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-jobs/etl_job.py \
        --input_path /opt/spark-data/landing/{{ ds }} \
        --output_path /opt/spark-data/gold/{{ ds }}
        """,
    )

    validate = PythonOperator(
        task_id="validate_gold_zone",
        python_callable=validate_gold_zone,
    )

    ingest_kafka >> spark_etl >> validate
