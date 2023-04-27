from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from macepa_plugin import PGSQL2CHUpsertOperator
from macepa_plugin import ClickHouseMultiSqlOperator


def populate_data_source_in_data_warehouse():
    with TaskGroup('populate_data_source_in_data_warehouse', tooltip='Populate the data source table in the data warehouse') as group:

        generate_ch_sql_from_pg_for_data_source = PGSQL2CHUpsertOperator(
            task_id='generate_ch_sql_from_pg_for_data_source',
            postgres_conn_id='postgres',
            ch_table_name='data_source',
            ch_pks=['id'],
            sql="select * from data_source where change = 'insert' or change ='update'",
            exclude_fields=['change'],
            output_file="data_source.sql"
        )

        import_data_source_into_ch = ClickHouseMultiSqlOperator(
            task_id='import_data_source_into_ch',
            database='core',
            clickhouse_conn_id='clickhouse',
            sql_file='dags/tmp/ch_sql/data_source.sql'
        )

        reset_data_source = PostgresOperator(
            task_id='reset_data_source',
            postgres_conn_id='postgres',
            sql="update data_source set change = '' where change = 'update' or change = 'insert'"
        )

        generate_ch_sql_from_pg_for_data_source >> import_data_source_into_ch >> reset_data_source

    return group
