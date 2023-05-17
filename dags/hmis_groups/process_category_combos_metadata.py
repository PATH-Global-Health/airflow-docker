
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from macepa_plugin import DHIS2MetadataDownloadOperator, GeneratePostgreSQLOperator, GeneratePostgreSQLMNOperator


def process_category_combos_metadata():
    with TaskGroup('process_category_combos_metadata', tooltip='Download and import category combos into the staging database') as group:
        extract_category_combos = DHIS2MetadataDownloadOperator(
            task_id='extract_category_combos',
            endpoint='categoryCombos',
            http_conn_id='hmis_dhis2_api',
            fields='*'
        )

        change_json_2_sql_category_combos = GeneratePostgreSQLOperator(
            task_id='change_json_2_sql_category_combos',
            table_name='categorycombo',
            json_key_table_columns_2_map={
                'id': {'column': 'uid', 'type': 'str'},
                'created': {'column': 'created', 'type': 'timestamp'},
                'lastUpdated': {'column': 'lastupdated', 'type': 'timestamp'},
                'name': {'column': 'name', 'type': 'str'}
            },
            primary_keys=[
                'uid', 'sourceid'
            ],
            output_sql_filename="categoryCombos.sql",
            input_json_file="dags/tmp/json/categoryCombos.json"
        )

        import_category_combos = PostgresOperator(
            task_id='import_category_combos',
            postgres_conn_id='postgres',
            sql="tmp/pg_sql/categoryCombos.sql"
        )

        change_json_2_sql_category_category_combos = GeneratePostgreSQLMNOperator(
            task_id='change_json_2_sql_category_category_combos',
            table_name='dataelementcategory_categorycombo',
            json_key_table_columns_2_map={
                'id': {'column': 'categorycombo_id', 'type': 'str'},
            },
            target_list_key_for_mn='categories',
            mn_json_key_table_columns_2_map={
                'id': {'column': 'dataelementcategory_id', 'type': 'str'},
            },
            primary_keys=[
                'categorycombo_id', 'dataelementcategory_id', 'sourceid'
            ],
            output_sql_filename="category-CategoryCombos.sql",
            input_json_file="dags/tmp/json/categoryCombos.json"
        )

        extract_category_combos >> [change_json_2_sql_category_combos, change_json_2_sql_category_category_combos] >> \
            import_category_combos

    return group
