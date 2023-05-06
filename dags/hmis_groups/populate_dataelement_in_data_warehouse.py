import os
import json

from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

from macepa_plugin import ClickHouseMultiSqlOperator, GenerateCHTableSchema


CH_DATAELEMENT_TABLE_SCHEMA = 'dags/tmp/ch_sql/dataElementSchema.sql'
DATAELEMENT_METADATA_JSON_FILE = 'dags/tmp/json/dataElementMetadata.json'
DATAELEMENT_METADATA_SQL_FILE = 'dags/tmp/ch_sql/dataElementMetadata.sql'


# Data structure for category metadata
# {
#     "dataelement_uid_1": {
#         "name": "dataelement name",
#         "source": "c1x82kksj"
#         "groups": {
#               "group_uid_1":"group name 1",
#               "group_uid_2":"group name 2",
#         }
#         ...
#     },
#     "dataelement_uid_2": {
#         "name": "dataelement name",
#         "source": "c1x82kksj"
#         "groups": {
#               "group_uid_1":"group name 1",
#               "group_uid_2":"group name 2",
#         }
#         ...
#     }
# }


def convert_dataelement_metadata(ti):
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_df = pg_hook.get_pandas_df("""
       select de.uid, de.name,deg.uid,deg.name, de.source_id from dataelement as de 
        inner join dataelement_dataelementgroup as de_deg on de.uid = de_deg.dataelement_id 
        inner join dataelementgroup as deg on deg.uid = de_deg.group_id;""")
    data = {}

    # row[0] - data element uid
    # row[1] - data element name
    # row[2] - data element group uid
    # row[3] - data element group name
    # row[4] - data source id
    for index, row in pg_df.iterrows():
        if row[0] not in data:
            data[row[0]] = {"name": row[1], "source": row[4], "groups": {}}

        data[row[0]]["groups"][row[2]] = row[3]

    with open(DATAELEMENT_METADATA_JSON_FILE, 'w') as file:
        file.write(json.dumps(data))


def populate_dataelement_in_data_warehouse():
    with TaskGroup('populate_data_element_in_data_warehouse', tooltip='Populate the dataelement table in the data warehouse') as group:
        generate_dataelement_columns_schema = GenerateCHTableSchema(
            task_id='generate_dataelement_columns_schema',
            ch_table_name='dataelement',
            postgres_conn_id='postgres',
            pg_table_name='dataelementgroup',
            output_file='dataElementSchema.sql'
        )

        import_dataelement_schema_into_ch = ClickHouseMultiSqlOperator(
            task_id='import_dataelement_schema_into_ch',
            database='core',
            clickhouse_conn_id='clickhouse',
            sql_file=CH_DATAELEMENT_TABLE_SCHEMA
        )

        reset_dataelement_in_pgsql = PostgresOperator(
            task_id='reset_dataelement_in_pgsql',
            postgres_conn_id='postgres',
            sql="update dataelementgroup set change = '' where change = 'update' or change = 'insert'"
        )

        convert_dataelement_metadata_to_json = PythonOperator(
            task_id='convert_dataelement_metadata_to_json',
            python_callable=convert_dataelement_metadata
        )

        generate_dataelement_columns_schema >> import_dataelement_schema_into_ch >> reset_dataelement_in_pgsql >> \
            convert_dataelement_metadata_to_json

    return group
