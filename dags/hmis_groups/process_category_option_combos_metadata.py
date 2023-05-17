
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from macepa_plugin import DHIS2MetadataDownloadOperator, GeneratePostgreSQLOperator, GeneratePostgreSQLMNOperator


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
            table_name='categoryoptioncombo',
            json_key_table_columns_2_map={
                'id': {'column': 'uid', 'type': 'str'},
                'created': {'column': 'created', 'type': 'timestamp'},
                'lastUpdated': {'column': 'lastupdated', 'type': 'timestamp'},
                'name': {'column': 'name', 'type': 'str'},
                'categoryCombo': {
                    'nestedColumns': {
                        'id': {'column': 'categorycombo_id', 'type': 'str'}
                    },
                }
            },
            primary_keys=[
                'uid', 'sourceid'
            ],
            output_sql_filename="categoryOptionCombos.sql",
            input_json_file="dags/tmp/json/categoryOptionCombos.json"
        )

        import_category_option_combos = PostgresOperator(
            task_id='import_category_option_combos',
            postgres_conn_id='postgres',
            sql="tmp/pg_sql/categoryOptionCombos.sql"
        )

        change_json_2_sql_categoryoptioncombo_categoryoptions = GeneratePostgreSQLMNOperator(
            task_id='change_json_2_sql_categoryoptioncombo_categoryoptions',
            table_name='categoryoptioncombo_categoryoptions',
            json_key_table_columns_2_map={
                'id': {'column': 'category_option_combo_id', 'type': 'str'},
            },
            target_list_key_for_mn='categoryOptions',
            mn_json_key_table_columns_2_map={
                'id': {'column': 'category_option_id', 'type': 'str'},
            },
            primary_keys=[
                'category_option_combo_id', 'category_option_id', 'sourceid'
            ],
            output_sql_filename="categoryOptionCombos_Categoryoptions.sql",
            input_json_file="dags/tmp/json/categoryOptionCombos.json"
        )

        import_categoryoptioncombos_categoryoptions = PostgresOperator(
            task_id='import_categoryoptioncombos_categoryoptions',
            postgres_conn_id='postgres',
            sql="tmp/pg_sql/categoryOptionCombos_Categoryoptions.sql"
        )

        extract_category_option_combos >> change_json_2_sql_category_option_combos >> \
            import_category_option_combos >> change_json_2_sql_categoryoptioncombo_categoryoptions >> \
            import_categoryoptioncombos_categoryoptions

    return group
