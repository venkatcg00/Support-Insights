from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import json
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'minio_listener_dag',
    default_args=default_args,
    description='Listens for MinIO notifications and triggers CSV processing DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    @task
    def process_notification():
        # This task would be triggered by MinIO via REST API
        # For simplicity, assume the notification payload is passed as a DAG run conf
        from airflow.operators.python import get_current_context
        context = get_current_context()
        conf = context['dag_run'].conf if context['dag_run'].conf else {}

        # Extract file info from MinIO notification
        records = conf.get('Records', [])
        if not records:
            logging.error("No Records in notification")
            return

        for record in records:
            bucket = record.get('s3', {}).get('bucket', {}).get('name')
            key = record.get('s3', {}).get('object', {}).get('key')
            if bucket and key and key.endswith('.csv'):
                # Trigger the processing DAG
                logging.info(f"Triggering DAG for file: {key} in bucket: {bucket}")
                response = requests.post(
                    'http://localhost:8080/api/v1/dags/process_csv_dag/dagRuns',
                    auth=('project_user', 'ProjectPassword#1'),
                    headers={'Content-Type': 'application/json'},
                    json={
                        'conf': {'bucket': bucket, 'key': key},
                    },
                )
                if response.status_code == 200:
                    logging.info(f"Triggered process_csv_dag for {key}")
                else:
                    logging.error(f"Failed to trigger DAG: {response.text}")

    process_notification()