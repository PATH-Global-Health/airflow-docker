
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from macepa_plugin import DHIS2MetadataDownloadOperator, GeneratePostgreSQLOperator


def process_category_option_combos_metadata():
    with TaskGroup('process_category_option_combos_metadata', tooltip='Download and import category option combos into the staging database') as group:
        extract_category_option_combos = DHIS2MetadataDownloadOperator(
            task_id='extract_category_option_combos',
            endpoint='categoryOptionCombos',
            http_conn_id='hmis_dhis2_api',
            fields='*'
        )

        change_json_2_sql_category_option_combos = GeneratePostgreSQLOperator(
            task_id='change_json_2_sql_category_option_combos',
            table_name='organisationunit',
            json_key_table_columns_2_map={
                'id': {'column': 'uid', 'type': 'str'},
                'created': {'column': 'created', 'type': 'timestamp'},
                'lastUpdated': {'column': 'lastupdated', 'type': 'timestamp'},
                'name': {'column': 'name', 'type': 'str'},
                'categoryCombo.id': {'column': 'categorycombo_id', 'type': 'str'}
            },
            primary_keys=[
                'uid', 'source_id'
            ],
            sql_filename="categoryOptionCombos.sql",
            json_file="dags/tmp/json/categoryOptionCombos.json"
        )

        import_category_option_combos = PostgresOperator(
            task_id='import_category_option_combos',
            postgres_conn_id='postgres',
            sql="tmp/pg_sql/categoryOptionCombos.sql"
        )

        extract_category_option_combos >> change_json_2_sql_category_option_combos >> import_category_option_combos

    return group
