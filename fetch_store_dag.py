doc_md = """
Code for airflow DAG which has two tasks, fetch_task will fetch the data from newsapi
and write_task will use the fetched data to write it in a bucket in google cloud storage.
This DAG will run on daily basis.

Before triggering this DAG, please set Admin Variable in following format:
Key: fetch_store_dag_config

Value: (This will be in JSON format)
{
  "GOOGLE_APPLICATION_CREDENTIALS": "path/to/google_credentials.json", # JSON credentials file for service account created in GCP
  "API_KEY":"XXXXXXXXXXXXXXXXXX", # Your newsapi API key
  "QUERY":"Manipur Violence" # Query to fetch news about
}
"""

from newsapi import NewsApiClient
from google.cloud import storage
from datetime import datetime
from pytz import timezone
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta


config_string = Variable.get("fetch_store_dag_config")
config = json.loads(config_string)

credentials_path = config["GOOGLE_APPLICATION_CREDENTIALS"]
api_key = config["API_KEY"]
query = config["QUERY"]

now_ist = datetime.now(timezone('Asia/Kolkata'))
date_ist = now_ist.strftime("%Y-%m-%d")

def fetch_data(**context):
    api = NewsApiClient(api_key=api_key)
    data = api.get_everything(q=f"{query}",from_param=date_ist,to=date_ist)
    context['ti'].xcom_push(key='data', value=data)

def write_data(**context):
    data = context['ti'].xcom_pull(key='data')
    client = storage.Client.from_service_account_json(credentials_path)
    bucket = client.get_bucket("wizikey")
    blob = bucket.blob(f"{date_ist}_{query}_data.json")
    blob.upload_from_string(data=json.dumps(data),content_type='application/json')

default_args = {
    'owner': 'Rushabh',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    doc_md=doc_md
)

fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

write_task = PythonOperator(
    task_id='write_data',
    python_callable=write_data,
    provide_context=True,
    dag=dag,
)

fetch_task >> write_task
