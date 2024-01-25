from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import mysql.connector

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mysql_to_bigquery',
    default_args=default_args,
    description='A simple DAG to move data from MySQL to BigQuery',
    schedule_interval=timedelta(days=1),
)

def mysql_to_bigquery():
    # MySQL connection
    cnx = mysql.connector.connect(user='guest', password='guest1234!1',
                                host='34.22.109.152', port = 3306,
                                database='solution')
    query = "SELECT * FROM death"
    df = pd.read_sql(query, cnx)

    # BigQuery connection
    credentials = service_account.Credentials.from_service_account_file(
        '/opt/airflow/dags/gootrend-5d6987d9756f.json')
    client = bigquery.Client(credentials=credentials, project='gootrend')

    # Upload to BigQuery
    dataset_ref = client.dataset('search')
    table_ref = dataset_ref.table('drug_death')
    job = client.load_table_from_dataframe(df, table_ref)

    job.result()  # Wait for the job to complete

t1 = PythonOperator(
    task_id='mysql_to_bigquery',
    python_callable=mysql_to_bigquery,
    dag=dag,
)

t1