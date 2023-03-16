
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from macepa_plugin import DHIS2MetadataDownloadOperator, GeneratePostgreSQLOperator


def process_org_units_metadata():
    with TaskGroup('process_org_units_metadata', tooltip='Download and import orgunits into the staging database') as group:
        extract_organisation_unit = DHIS2MetadataDownloadOperator(
            task_id='extract_organisation_unit',
            endpoint='organisationUnits',
            http_conn_id='hmis_dhis2_api',
            fields='*'
        )

        change_json_2_sql_org_unit = GeneratePostgreSQLOperator(
            task_id='change_json_2_sql_org_unit',
            table_name='organisationunit',
            json_key_table_columns_2_map={
                'id': {'column': 'uid', 'type': 'str'},
                'code': {'column': 'code', 'type': 'str'},
                'created': {'column': 'created', 'type': 'timestamp'},
                'lastUpdated': {'column': 'lastupdated', 'type': 'timestamp'},
                'name': {'column': 'name', 'type': 'str'},
                'shortName': {'column': 'shortname', 'type': 'str'},
                'parent.id': {'column': 'parentid', 'type': 'str'},
                'path': {'column': 'path', 'type': 'str'},
                'leaf': {'column': 'leaf', 'type': 'bool'},
                'openingDate': {'column': 'openingdate', 'type': 'date'},
                'closedDate': {'column': 'closeddate', 'type': 'date'},
                'contactPerson': {'column': 'contactperson', 'type': 'str'},
                'address': {'column': 'address', 'type': 'str'},
                'email': {'column': 'email', 'type': 'str'},
                'phoneNumber': {'column': 'phonenumber', 'type': 'str'},
                'geometry': {
                    'nestedColumns': {
                        'type': {'column': 'featuretype', 'type': 'str'},
                        'coordinates': {'column': 'coordinates', 'type': 'str'}
                    },
                }
            },
            primary_keys=[
                'uid', 'source_id'
            ],
            sql_filename="organisationUnits.sql",
            json_file="dags/tmp/json/organisationUnits.json"
        )

        import_org_units = PostgresOperator(
            task_id='import_org_units',
            postgres_conn_id='postgres',
            sql="tmp/pg_sql/organisationUnits.sql"
        )

        extract_organisation_unit >> change_json_2_sql_org_unit >> import_org_units

    return group
