import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['bserda@path.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('HMIS_staging_etl',  default_args=default_args, 
         description='A pipeline for reading data from HMIS/DHIS2 and storing it in a PostgreSQL staging database.', 
         schedule_interval='0 19 * * *', catchup=False) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql="sql/pg_create_table"
    )