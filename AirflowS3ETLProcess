from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import boto3
import pandas as pd
import requests

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='An ETL pipeline that extracts data from Salesforce, transforms it, and loads it to S3',
    schedule_interval=timedelta(days=1),
)

def extract_data_from_salesforce(**kwargs):
    # Example function to extract data from Salesforce
    salesforce_url = 'https://example.salesforce.com/data'
    response = requests.get(salesforce_url, headers={'Authorization': 'Bearer YOUR_ACCESS_TOKEN'})
    data = response.json()
    df = pd.DataFrame(data)
    df.to_csv('/tmp/salesforce_data.csv', index=False)

def transform_data(**kwargs):
    # Example function to transform data
    df = pd.read_csv('/tmp/salesforce_data.csv')
    # Add your transformation logic here
    df['new_column'] = df['existing_column'] * 2
    df.to_csv('/tmp/transformed_data.csv', index=False)

def load_data_to_s3(**kwargs):
    # Function to load data to S3
    s3_hook = BaseHook.get_connection('aws_default')
    s3_client = boto3.client(
        's3',
        aws_access_key_id=s3_hook.login,
        aws_secret_access_key=s3_hook.password,
    )
    with open('/tmp/transformed_data.csv', 'rb') as data:
        s3_client.upload_fileobj(data, 'your-s3-bucket-name', 'transformed_data.csv')

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_from_salesforce,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_s3,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
