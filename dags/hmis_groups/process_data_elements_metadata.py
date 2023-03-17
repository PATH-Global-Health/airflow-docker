
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from macepa_plugin import DHIS2MetadataDownloadOperator, GeneratePostgreSQLOperator, GeneratePostgreSQLMNOperator


def process_data_elements_metadata():
    with TaskGroup('process_data_elements_metadata', tooltip='Download and import data elements into the staging database') as group:
        extract_data_element = DHIS2MetadataDownloadOperator(
            task_id='extract_data_element',
            endpoint='dataElements',
            http_conn_id='hmis_dhis2_api',
            fields='*'
        )

        change_json_2_sql_data_element = GeneratePostgreSQLOperator(
            task_id='change_json_2_sql_data_element',
            table_name='dataelement',
            json_key_table_columns_2_map={
                'id': {'column': 'uid', 'type': 'str'},
                'code': {'column': 'code', 'type': 'str'},
                'created': {'column': 'created', 'type': 'timestamp'},
                'lastUpdated': {'column': 'lastupdated', 'type': 'timestamp'},
                'name': {'column': 'name', 'type': 'str'},
                'shortName': {'column': 'shortname', 'type': 'str'},
                'formName': {'column': 'formname', 'type': 'str'},
                'valueType': {'column': 'valuetype', 'type': 'str'},
                'domainType': {'column': 'domaintype', 'type': 'str'},
                'aggregationType': {'column': 'aggregationtype', 'type': 'str'},
                'categoryCombo': {
                    'nestedColumns': {
                        'id': {'column': 'categorycomboid', 'type': 'str'},
                    },
                },
                'href': {'column': 'url', 'type': 'str'}
            },
            primary_keys=[
                'uid', 'source_id'
            ],
            output_sql_filename="dataElements.sql",
            input_json_file="dags/tmp/json/dataElements.json"
        )

        change_json_2_sql_dataelement_dataelementgroup = GeneratePostgreSQLMNOperator(
            task_id='change_json_2_sql_dataelement_dataelementgroup',
            table_name='dataelement_dataelementgroup',
            json_key_table_columns_2_map={
                'id': {'column': 'dataelement_id', 'type': 'str'},
            },
            target_list_key_for_mn='dataElementGroups',
            mn_json_key_table_columns_2_map={
                'id': {'column': 'group_id', 'type': 'str'},
            },
            primary_keys=[
                'dataelement_id', 'group_id', 'source_id'
            ],
            output_sql_filename="dataElement-dataElementGroup.sql",
            input_json_file="dags/tmp/json/dataElements.json"
        )

        import_data_element = PostgresOperator(
            task_id='import_data_element',
            postgres_conn_id='postgres',
            sql="tmp/pg_sql/dataElements.sql"
        )

        import_dataelement_dataelementgroup = PostgresOperator(
            task_id='import_dataelement_dataelementgroup',
            postgres_conn_id='postgres',
            sql="tmp/pg_sql/dataElement-dataElementGroup.sql"
        )

        extract_data_element >> [change_json_2_sql_data_element,
                                 change_json_2_sql_dataelement_dataelementgroup] >> \
            import_data_element >> import_dataelement_dataelementgroup

    return group
