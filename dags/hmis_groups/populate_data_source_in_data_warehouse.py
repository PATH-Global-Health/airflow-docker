
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from macepa_plugin import PGSQL2CHUpsertOperator


def populate_data_source_in_data_warehouse():
    with TaskGroup('populate_data_source_in_data_warehouse', tooltip='Populate the data source table in the data warehouse') as group:

        genereate_ch_sql_from_pg_for_data_source = PGSQL2CHUpsertOperator(
            task_id='genereate_ch_sql_from_pg_for_data_source',
            postgres_conn_id='postgres',
            ch_table_name='data_source',
            ch_pks=['id'],
            sql='select * from data_source',
            exclude_fields=['change'],
            output_file="dataSource.sql"
        )

        genereate_ch_sql_from_pg_for_data_source

    return group
