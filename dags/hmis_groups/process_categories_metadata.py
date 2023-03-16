
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from macepa_plugin import DHIS2MetadataDownloadOperator, GeneratePostgreSQLOperator


def process_categories_metadata():
    with TaskGroup('process_categories_metadata', tooltip='Download and import data element categories into the staging database') as group:
        extract_categories = DHIS2MetadataDownloadOperator(
            task_id='extract_categories',
            endpoint='categories',
            http_conn_id='hmis_dhis2_api',
            fields='*'
        )

        change_json_2_sql_categories = GeneratePostgreSQLOperator(
            task_id='change_json_2_sql_categories',
            table_name='dataelementcategory',
            json_key_table_columns_2_map={
                'id': {'column': 'uid', 'type': 'str'},
                'created': {'column': 'created', 'type': 'timestamp'},
                'lastUpdated': {'column': 'lastupdated', 'type': 'timestamp'},
                'name': {'column': 'name', 'type': 'str'},
                'dataDimension': {'column': 'datadimension', 'type': 'bool'}
            },
            primary_keys=[
                'uid', 'source_id'
            ],
            output_sql_filename="categories.sql",
            input_json_file="dags/tmp/json/categories.json"
        )

        import_categories = PostgresOperator(
            task_id='import_categories',
            postgres_conn_id='postgres',
            sql="tmp/pg_sql/categories.sql"
        )

        extract_categories >> change_json_2_sql_categories >> import_categories

    return group
