
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from macepa_plugin import DHIS2MetadataDownloadOperator, GeneratePostgreSQLOperator


def process_data_element_groups_metadata():
    with TaskGroup('process_data_element_groups_metadata', tooltip='Download and import data element groups into the staging database') as group:
        extract_data_element_group = DHIS2MetadataDownloadOperator(
            task_id='extract_data_element_group',
            endpoint='dataElementGroups',
            http_conn_id='hmis_dhis2_api',
            fields='*'
        )

        change_json_2_sql_data_element_group = GeneratePostgreSQLOperator(
            task_id='change_json_2_sql_data_element_group',
            table_name='dataelementgroup',
            json_key_table_columns_2_map={
                'id': {'column': 'uid', 'type': 'str'},
                'created': {'column': 'created', 'type': 'timestamp'},
                'lastUpdated': {'column': 'lastupdated', 'type': 'timestamp'},
                'name': {'column': 'name', 'type': 'str'},
                'displayFormName': {'column': 'formname', 'type': 'str'},
                'aggregationType': {'column': 'aggregationtype', 'type': 'str'}
            },
            primary_keys=[
                'uid', 'source_id'
            ],
            output_sql_filename="dataElementGroups.sql",
            input_json_file="dags/tmp/json/dataElementGroups.json"
        )

        import_data_element_group = PostgresOperator(
            task_id='import_data_element_group',
            postgres_conn_id='postgres',
            sql="tmp/pg_sql/dataElementGroups.sql"
        )

        extract_data_element_group >> change_json_2_sql_data_element_group >> import_data_element_group

    return group
