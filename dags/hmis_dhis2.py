import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook

from datetime import timedelta

from hmis_groups.process_org_units_metadata import process_org_units_metadata

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['bserda@path.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(days=15),
}

# ti - task instance


def _set_data_source(ti, http_conn_id):
    connection = BaseHook.get_connection(http_conn_id)
    url = connection.host
    ti.xcom_push(key="get_hmis_data_source", value={
                 'id': 'source_id', 'url': url})


with DAG('HMIS-DHIS2',  default_args=default_args,
         description='''A pipeline for reading data from HMIS/DHIS2 and storing it in a
         PostgreSQL staging database, after which the data is transformed and stored in the ClickHouse.''',
         schedule='0 19 * * *', catchup=False) as dag:

    create_staging_tables = PostgresOperator(
        task_id='create_staging_tables',
        postgres_conn_id='postgres',
        sql="sql/pg_create_tables.sql"
    )

    populate_data_source_tables = PostgresOperator(
        task_id='populate_data_source_tables',
        postgres_conn_id='postgres',
        sql="sql/pg_data_source_table.sql"
    )

    set_data_source = PythonOperator(
        task_id='set_data_source',
        python_callable=_set_data_source,
        op_kwargs={"http_conn_id": 'hmis_dhis2_api'},
    )

    process_hmis_org_units_metadata = process_org_units_metadata()

    create_staging_tables >> populate_data_source_tables >> set_data_source >> \
        process_hmis_org_units_metadata
