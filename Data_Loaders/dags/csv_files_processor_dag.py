from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
import re
from datetime import timedelta
import os
import pandas as pd
import uuid
import shutil
from Batch_Processing import CSV_Batch_Processor as cbp

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    'csv_files_processor_dag',
    default_args=default_args,
    description='Checks MinIO for new CSV files every 15 minutes and triggers processing',
    schedule_interval=timedelta(minutes=15),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    @task(task_id='get_last_loaded_record_id')
    def get_last_loaded_record_id():
        mysql_hook = MySqlHook(mysql_conn_id='mysql_project_connection')
        query = """
        SELECT COALESCE(MAX(LAST_LOADED_RECORD_ID), 0)
        FROM CSD_SOURCES
        WHERE LOWER(SOURCE_FILE_TYPE) = 'csv' AND ACTIVE_FLAG = 1 AND SOURCE_NAME = 'AT&T'
        """
        return mysql_hook.get_first(query)[0]

    @task(task_id='check_and_merge_files')
    def check_and_merge_files(last_id):
        s3_hook = S3Hook(aws_conn_id='minio_project_connection')
        bucket_name = Variable.get('minio_bucket')
        
        objects = s3_hook.list_keys(bucket_name=bucket_name, prefix='') or []
        pattern = re.compile(r'^(\d+)_AT&T_Data_.*\.csv$')
        
        new_files = [
            {'key': key, 'id': int(match.group(1))}
            for key in objects
            if (match := pattern.match(key)) and int(match.group(1)) > last_id
        ]
        
        if not new_files:
            return None
            
        new_files.sort(key=lambda x: x['id'])
        highest_id = max(file['id'] for file in new_files)
        temp_dir = f"/tmp/airflow_minio_{uuid.uuid4().hex}"
        os.makedirs(temp_dir, exist_ok=True)
        
        merged_path = os.path.join(temp_dir, f"merged_{highest_id}.csv")
        first_file = True
        for file in new_files:
            local_file_path = s3_hook.download_file(
                key=file['key'],
                bucket_name=bucket_name,
                local_path=temp_dir
            )
            if not os.path.isfile(local_file_path):
                raise FileNotFoundError(f"Downloaded file not found at {local_file_path}")
            df = pd.read_csv(local_file_path, sep='|')
            with open(merged_path, 'a') as f:
                df.to_csv(f, sep='|', index=False, header=first_file)
            os.remove(local_file_path)
            first_file = False
            
        return {
            'merged_path': merged_path,
            'highest_id': highest_id,
            'temp_dir': temp_dir
        }

    def decide_next_step(ti):
        processing_info = ti.xcom_pull(task_ids='check_and_merge_files')
        return 'process_branch' if processing_info else 'skip_branch'

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=decide_next_step,
        provide_context=True
    )

    # Process Branch
    process_branch = DummyOperator(task_id='process_branch')

    @task(task_id='ETL_process')
    def etl_process(ti):
        processing_info = ti.xcom_pull(task_ids='check_and_merge_files')
        if not processing_info:
            return None
            
        dag_run_id = ti.dag_run.run_id
        merged_path = processing_info['merged_path']
        return cbp.whole_etl_process(dag_run_id, merged_path, 'mysql_project_connection')

    @task(task_id='update_and_cleanup')
    def update_and_cleanup(ti):
        processing_info = ti.xcom_pull(task_ids='check_and_merge_files')
        if not processing_info:
            return
            
        mysql_hook = MySqlHook(mysql_conn_id='mysql_project_connection')
        mysql_hook.run(
            """
            UPDATE CSD_SOURCES
            SET LAST_LOADED_RECORD_ID = %s
            WHERE SOURCE_FILE_TYPE = 'csv' AND ACTIVE_FLAG = 1 AND SOURCE_NAME = 'AT&T'
            """,
            parameters=(processing_info['highest_id'],)
        )
        
        shutil.rmtree(processing_info['temp_dir'])

    # Skip Branch
    skip_branch = DummyOperator(task_id='skip_branch')

    # Separate end tasks for each branch
    end_dag_process = DummyOperator(task_id='end_dag_process')
    end_dag_skip = DummyOperator(task_id='end_dag_skip')

    # Task flow
    last_id_task = get_last_loaded_record_id()
    check_merge_task = check_and_merge_files(last_id_task)
    last_id_task >> check_merge_task >> branch_task

    # Process branch flow
    branch_task >> process_branch
    etl_task = etl_process()
    update_cleanup_task = update_and_cleanup()
    process_branch >> etl_task >> update_cleanup_task >> end_dag_process

    # Skip branch flow
    branch_task >> skip_branch
    skip_branch >> end_dag_skip