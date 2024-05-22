from datetime import timedelta,datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
from  finance_etl import yahoo_finance_extraction, create_table, insert_data

dag_path = os.getcwd()
default_args = {
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

ingestion_dag = DAG(
    dag_id='ingestion_data',
    default_args=default_args,
    description='ETL para obtener Stock information de 10 empresas en Yahoo Finance',
     schedule_interval=timedelta(days=1),
    catchup=False
)


task_1 = PythonOperator(
    task_id='yahoo_extraction',
    python_callable=yahoo_finance_extraction,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=ingestion_dag,
)

task_3 = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=ingestion_dag,
)



task_1 >> task_2 >> task_3
