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

        generate_dataelement_columns_schema >> import_dataelement_schema_into_ch >> reset_dataelement_in_pgsql

    return group
