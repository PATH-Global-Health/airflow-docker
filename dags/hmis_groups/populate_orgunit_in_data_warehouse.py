import os

from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

from macepa_plugin import PGSQL2CHInsertOperator, PGSQL2JSONOperator, ClickHouseMultiSqlOperator
from helpers.utils import query_and_push

CH_ORGUNIT_TABLE = "orgunit"
CH_ORGUNIT_TABLE_SCHEMA = "dags/tmp/ch_sql/org_unit_schema.sql"

def generate_orgunit_schema(ti):
    levels = ti.xcom_pull(key='get_org_unit_levels')
    sql = []
    previous_field_name = ""
    for index, level in enumerate(levels):
        if level[4] == "insert":
            if index == 0:
                sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER last_updated;'.format(CH_ORGUNIT_TABLE, level[0]))
            else:
                 sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(CH_ORGUNIT_TABLE, level[0], previous_field_name))
            sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(CH_ORGUNIT_TABLE, level[1].replace(" ", ""), level[0]))
            previous_field_name = level[1].replace(" ", "")
        elif level[4] == "update":
            sql.append('ALTER TABLE {} RENAME COLUMN IF EXISTS {} to {}'.format(CH_ORGUNIT_TABLE, level[2].replace(" ", ""), level[1].replace(" ", "")))

    with open(os.path.join(CH_ORGUNIT_TABLE_SCHEMA), 'w') as f:
            f.write('\n'.join(sql))


def populate_orgunit_in_data_warehouse():
    with TaskGroup('populate_orgunit_in_data_warehouse', tooltip='Populate the orgunit table in the data warehouse') as group:

        get_org_unit_levels = PythonOperator(
            task_id='get_org_unit_levels',
            python_callable=query_and_push,
            provide_context=True,
            op_kwargs={
                'sql': "select uid, name, previous_name, level, change from orgunitlevel where change='insert' or change='update' order by level;",
                'postgres_conn_id': 'postgres',
                'key': 'get_org_unit_levels'
            }
        )

        generate_orgunit_columns_schema = PythonOperator(
            task_id='generate_orgunit_columns_schema',
            python_callable=generate_orgunit_schema
        )

        import_orgunit_schema_into_ch = ClickHouseMultiSqlOperator(
            task_id='import_orgunit_schema_into_ch',
            database='core',
            clickhouse_conn_id='clickhouse',
            sql_file=CH_ORGUNIT_TABLE_SCHEMA
        )

        reset_orgunit_level_in_pgsql = PostgresOperator(
            task_id='reset_orgunit_level_in_pgsql',
            postgres_conn_id='postgres',
            sql="update orgunitlevel set change = '' where change = 'update' or change = 'insert'"
        )

        export_orgunit_from_pgsql_2_json = PGSQL2JSONOperator(
            task_id='export_orgunit_from_pgsql_2_json',
            postgres_conn_id='postgres',
            sql="select uid, name, parentid, path, source_id, change from organisationunit where change = 'insert' or change ='update'",
            unique_keys=['uid'],
            output_file="orgunits.json"
        )
        


        # generate_ch_sql_from_pg_for_orgunit = PGSQL2CHInsertOperator(
        #     task_id='generate_ch_sql_from_pg_for_orgunit',
        #     postgres_conn_id='postgres',
        #     ch_table_name='orgunit',
        #     ch_pks=['uid'],
        #     sql="select * from organisationunit where change = 'insert' or change ='update'",
        #     exclude_fields=['change'],
        #     output_file="orgunit.sql"
        # )

        # import_orgunit_into_ch = ClickHouseMultiSqlOperator(
        #     task_id='import_orgunit_into_ch',
        #     database='core',
        #     clickhouse_conn_id='clickhouse',
        #     sql_file='dags/tmp/ch_sql/data/orgunit.sql'
        # )

        # reset_orgunit = PostgresOperator(
        #     task_id='reset_data_source',
        #     postgres_conn_id='postgres',
        #     sql="update organisationunit set change = '' where change = 'update' or change = 'insert'"
        # )

        get_org_unit_levels >> generate_orgunit_columns_schema >> import_orgunit_schema_into_ch >> \
            reset_orgunit_level_in_pgsql >> export_orgunit_from_pgsql_2_json

    return group
