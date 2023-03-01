import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from macepa_plugin import DHIS2MetadataDownloadOperator

import json
from pandas import json_normalize
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

# ti - task instance


# def extract_organisation_unit():
#     for pages in api.get_paged('organisationUnits', page_size=100, params={'fields': 'id,displayName,parent'}):
#         for org_unit in pages['organisationUnits']:


def _process_organisation_units(ti):
    ou = ti.xcom_pull(task_ids="extract_organisation_unit")
    ou1 = ou['organisationUnits'][0]
    processed_ou = json_normalize({
        'uid': ou1['id'],
        'name': ou1['displayName']
    })

    processed_ou.to_csv(
        'dags/tmp/processed_organisation_units.csv', index=False, header=False)


def _store_organisation_unit():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY organisationunit FROM stdin WITH DELIMITER as ','",
        filename='dags/tmp/processed_organisation_units.csv'
    )


with DAG('HMIS-DHIS2',  default_args=default_args,
         description='''A pipeline for reading data from HMIS/DHIS2 and storing it in a 
         PostgreSQL staging database, after which the data is transformed and stored in the ClickHouse.''',
         schedule_interval='0 19 * * *', catchup=False) as dag:

    create_staging_tables = PostgresOperator(
        task_id='create_staging_tables',
        postgres_conn_id='postgres',
        sql="sql/pg_create_tables.sql"
    )

    extract_organisation_unit = SimpleHttpOperator(
        task_id='extract_organisation_unit',
        http_conn_id='hmis_dhis2_api',
        endpoint='api/organisationUnits',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_organisation_unit = PythonOperator(
        task_id='process_organisation_units',
        python_callable=_process_organisation_units
    )

    store_organisation_unit = PythonOperator(
        task_id='store_organisation_unit',
        python_callable=_store_organisation_unit
    )

    download_metadata = DHIS2MetadataDownloadOperator(
        task_id='download_metadata',
        endpoint='api/organisationUnits',
        http_conn_id='hmis_dhis2_api',
    )

    create_staging_tables >> extract_organisation_unit >> process_organisation_unit >> download_metadata
