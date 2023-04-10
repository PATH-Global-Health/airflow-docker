
from datetime import datetime
import glob

from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
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


def _check_json_data_available(ti):
    if glob.glob("dags/tmp/pg_sql/data/*.sql").__len__() > 0:
        return 'process_data.import_data_2_staging_db'


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
            python_callable=_set_hmis_last_updated,
            trigger_rule='none_failed'
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

        is_json_data_available = BranchPythonOperator(
            task_id='is_data_available',
            python_callable=_check_json_data_available
        )

        # PostgresOperator gets the sql files in tmp/pg_sql... and not in dags/tmp/pg_sql...
        # However glob.glob(...) gets the sql files in dags/tmp/pg_sql not in tmp/pg_sql...
        # That is why we replace dags/ with empty string to make PostgresOperator get the sql files.
        import_data_2_staging_db = PostgresOperator(task_id='import_data_2_staging_db',
                                                    postgres_conn_id='postgres',
                                                    sql=[x.replace('dags/', '') for x in glob.glob("dags/tmp/pg_sql/data/*.sql")])

        [get_hmis_last_updated,
            get_org_unit_ids] >> download_data >> change_hmis_data_from_json_2_sql >> is_json_data_available >> [import_data_2_staging_db] >> set_hmis_last_updated

    return group
