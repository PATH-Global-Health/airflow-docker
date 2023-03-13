import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from macepa_plugin import DHIS2MetadataDownloadOperator, GeneratePostgreSQLOperator

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


def _store_organisation_unit():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY organisationunit FROM stdin WITH DELIMITER as ','",
        filename='dags/tmp/processed_organisation_units.csv'
    )


with DAG('HMIS-DHIS2',  default_args=default_args,
         description='''A pipeline for reading data from HMIS/DHIS2 and storing it in a 
         PostgreSQL staging database, after which the data is transformed and stored in the ClickHouse.''',
         schedule='0 19 * * *', catchup=False) as dag:

    create_staging_tables = PostgresOperator(
        task_id='create_staging_tables',
        postgres_conn_id='postgres',
        sql="sql/pg_create_tables.sql"
    )

    extract_organisation_unit = DHIS2MetadataDownloadOperator(
        task_id='extract_organisation_unit',
        endpoint='organisationUnits',
        http_conn_id='hmis_dhis2_api',
        fields='id,code,name'
    )

    change_json_2_sql_org_unit = GeneratePostgreSQLOperator(
        task_id='change_json_2_sql_org_unit',
        table_name='organisationunit',
        json_key_table_columns_2_map={
            'id': {'column': 'uid', 'type': 'str'},
            'code': {'column': 'code', 'type': 'str'},
            'created': {'column': 'created', 'type': 'timestamp'},
            'lastUpdated': {'column': 'lastupdated', 'type': 'timestamp'},
            'name': {'column': 'name', 'type': 'str'},
            'shortName': {'column': 'shortname', 'type': 'str'},
            'parent.id': {'column': 'parentid', 'type': 'str'},
            'path': {'column': 'path', 'type': 'str'},
            'leaf': {'column': 'leaf', 'type': 'bool'},
            'openingDate': {'column': 'openingdate', 'type': 'date'},
            'closedDate': {'column': 'closeddate', 'type': 'date'},
            'contactPerson': {'column': 'contactperson', 'type': 'str'},
            'address': {'column': 'address', 'type': 'str'},
            'email': {'column': 'email', 'type': 'str'},
            'phoneNumber': {'column': 'phonenumber', 'type': 'str'},
            'geometry.type': {'column': 'featuretype', 'type': 'str'},
            'geometry.coordinates': {'column': 'coordinates', 'type': 'str'}
        },
        primary_keys=[
            'uid', 'source_id'
        ],
        sql_filename="organisationUnits.sql",
        json_file="dags/tmp/json/organisationUnits.json"
    )

    create_staging_tables >> extract_organisation_unit >> change_json_2_sql_org_unit
