
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from macepa_plugin import DHIS2MetadataDownloadOperator, GeneratePostgreSQLOperator, GeneratePostgreSQLMNOperator


def process_category_options_metadata():
    with TaskGroup('process_category_options_metadata', tooltip='Download and import category options into the staging database') as group:
        extract_category_options = DHIS2MetadataDownloadOperator(
            task_id='extract_category_options',
            endpoint='categoryOptions',
            http_conn_id='hmis_dhis2_api',
            fields='*'
        )

        change_json_2_sql_category_options = GeneratePostgreSQLOperator(
            task_id='change_json_2_sql_category_options',
            table_name='dataelementcategoryoption',
            json_key_table_columns_2_map={
                'id': {'column': 'uid', 'type': 'str'},
                'created': {'column': 'created', 'type': 'timestamp'},
                'lastUpdated': {'column': 'lastupdated', 'type': 'timestamp'},
                'name': {'column': 'name', 'type': 'str'},
                'shortName': {'column': 'shortname', 'type': 'str'}
            },
            primary_keys=[
                'uid', 'sourceid'
            ],
            output_sql_filename="categoryOptions.sql",
            input_json_file="dags/tmp/json/categoryOptions.json"
        )

        change_json_2_sql_category_category_options = GeneratePostgreSQLMNOperator(
            task_id='change_json_2_sql_category_category_options',
            table_name='dataelementcategory_categoryoption',
            json_key_table_columns_2_map={
                'id': {'column': 'categoryoption_id', 'type': 'str'},
            },
            target_list_key_for_mn='categories',
            mn_json_key_table_columns_2_map={
                'id': {'column': 'category_id', 'type': 'str'},
            },
            primary_keys=[
                'category_id', 'categoryoption_id', 'sourceid'
            ],
            output_sql_filename="category-CategoryOptions.sql",
            input_json_file="dags/tmp/json/categoryOptions.json"
        )

        import_category_options = PostgresOperator(
            task_id='import_category_options',
            postgres_conn_id='postgres',
            sql="tmp/pg_sql/categoryOptions.sql"
        )

        extract_category_options >> \
            [change_json_2_sql_category_options, change_json_2_sql_category_category_options] >> \
            import_category_options

    return group
