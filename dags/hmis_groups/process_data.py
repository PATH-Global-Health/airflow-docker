
from datetime import datetime

from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from macepa_plugin import GenerateMassPostgreSQLOperator, DHIS2DataDownloadOperator


def _get_hmis_last_updated(ti):
    hmis_last_updated = Variable.get(
        "hmis_last_updated", default_var='2018-01-01')
    ti.xcom_push(key="get_hmis_last_updated", value=hmis_last_updated)
    # return hmis_last_updated


def _set_hmis_last_updated():
    Variable.set("hmis_last_updated", datetime.today().strftime('%Y-%m-%d'))


def query_and_push(ti, sql, postgres_conn_id):
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    records = pg_hook.get_records(sql=sql)
    ti.xcom_push(key="get_org_unit_ids", value=records)
    # return records


def process_data():
    with TaskGroup('process_data', tooltip='Download and import data into the staging database') as group:
        get_hmis_last_updated = PythonOperator(
            task_id='get_hmis_last_updated',
            python_callable=_get_hmis_last_updated
        )

        get_org_unit_ids = PythonOperator(
            task_id='get_org_unit_ids',
            python_callable=query_and_push,
            provide_context=True,
            op_kwargs={
                'sql': "select uid from organisationunit where leaf='t';",
                'postgres_conn_id': 'postgres'
            }
        )

        set_hmis_last_updated = PythonOperator(
            task_id='set_hmis_last_updated',
            python_callable=_set_hmis_last_updated
        )

        download_data = DHIS2DataDownloadOperator(
            task_id='download_data',
            http_conn_id='hmis_dhis2_api'
        )

        change_hmis_data_from_json_2_sql = GenerateMassPostgreSQLOperator(
            task_id='change_hmis_data_from_json_2_sql',
            table_name='datavalue',
            json_key_table_columns_2_map={
                'dataElement': {'column': 'dataelementid', 'type': 'str'},
                'created': {'column': 'created', 'type': 'timestamp'},
                'lastUpdated': {'column': 'lastupdated', 'type': 'timestamp'},
                'period': {'column': 'period', 'type': 'str'},
                'orgUnit': {'column': 'organisationunitid', 'type': 'str'},
                'categoryOptionCombo': {'column': 'categoryoptioncomboid', 'type': 'str'},
                'attributeOptionCombo': {'column': 'attributeoptioncomboid', 'type': 'str'},
                'value': {'column': 'value', 'type': 'str'},
                'storedBy': {'column': 'storedby', 'type': 'str'},
                'followup': {'column': 'followup', 'type': 'bool'}
            },
            primary_keys=[
                'dataelementid', 'period', 'organisationunitid', 'categoryoptioncomboid', 'attributeoptioncomboid', 'source_id'
            ]
        )

        [get_hmis_last_updated,
            get_org_unit_ids] >> download_data >> change_hmis_data_from_json_2_sql >> set_hmis_last_updated

    return group
