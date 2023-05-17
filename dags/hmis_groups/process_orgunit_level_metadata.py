
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from macepa_plugin import DHIS2MetadataDownloadOperator, GeneratePostgreSQLOperator


def process_orgunit_level_metadata():
    with TaskGroup('process_orgunit_level_metadata', tooltip='Download and import orgunit level into the staging database') as group:

        extract_organisation_unit_levels = DHIS2MetadataDownloadOperator(
            task_id='extract_organisation_unit_levels',
            endpoint='organisationUnitLevels',
            http_conn_id='hmis_dhis2_api',
            fields='*'
        )

        change_json_2_sql_orgunit_level = GeneratePostgreSQLOperator(
            task_id='change_json_2_sql_orgunit_level',
            table_name='orgunitlevel',
            json_key_table_columns_2_map={
                'id': {'column': 'uid', 'type': 'str'},
                'code': {'column': 'code', 'type': 'str'},
                'created': {'column': 'created', 'type': 'timestamp'},
                'lastUpdated': {'column': 'lastupdated', 'type': 'timestamp'},
                'name': {'column': 'name', 'type': 'str'},
                'level': {'column': 'level', 'type': 'int'}
            },
            primary_keys=[
                'uid', 'sourceid'
            ],
            output_sql_filename="organisationUnitLevels.sql",
            input_json_file="dags/tmp/json/organisationUnitLevels.json"
        )

        import_orgunit_level = PostgresOperator(
            task_id='import_orgunit_level',
            postgres_conn_id='postgres',
            sql="tmp/pg_sql/organisationUnitLevels.sql"
        )

        extract_organisation_unit_levels >> change_json_2_sql_orgunit_level >> import_orgunit_level

    return group
