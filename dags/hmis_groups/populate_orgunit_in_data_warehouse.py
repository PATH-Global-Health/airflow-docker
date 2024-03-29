import os
import json
import csv
import pandas as pd
from datetime import datetime

from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

from macepa_plugin import PGSQL2JSONOperator, ClickHouseMultiSqlOperator, LoadCSV2CHOperator
from helpers.utils import query_and_push

CH_ORGUNIT_TABLE = "orgunit"
CH_ORGUNIT_TABLE_SCHEMA = "dags/tmp/ch_sql/org_unit_schema.sql"
ORG_UNIT_DIR_JSON = "dags/tmp/json"
ORG_UNIT_JSON = "orgunits.json"
ORG_UNIT_DIR_CSV = "dags/tmp/csv"
ORG_UNIT_HIERARCHY_CSV = "orgunits.csv"
DAG_FULL_PATH = '/Users/belendia/Documents/Projects/AirflowProj/docker-airflow'


def generate_orgunit_schema(ti):
    levels = ti.xcom_pull(key='get_org_unit_levels')
    sql = []
    previous_field_name = ""
    for index, level in enumerate(levels):
        if level[4] == "insert":
            if index == 0:
                sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER lastupdated;'.format(
                    CH_ORGUNIT_TABLE, level[0]))
            else:
                sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(
                    CH_ORGUNIT_TABLE, level[0], previous_field_name))
            sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(
                CH_ORGUNIT_TABLE, level[1].replace(" ", ""), level[0]))
            previous_field_name = level[1].replace(" ", "")
        elif level[4] == "update":
            sql.append('ALTER TABLE {} RENAME COLUMN IF EXISTS {} to {}'.format(
                CH_ORGUNIT_TABLE, level[2].replace(" ", ""), level[1].replace(" ", "")))

    with open(os.path.join(CH_ORGUNIT_TABLE_SCHEMA), 'w') as f:
        f.write('\n'.join(sql))


def generate_org_unit_hierarchy_in_csv(ti):
    levels = ti.xcom_pull(key='get_org_unit_levels_id_name')
    ou_levels = {}
    # organize levels in the form
    # {
    #   "1": {
    #       "id": "ues21LMtV",
    #       "name": "National"
    #   }
    #   "2": {
    #       "id": "ckt35MMtc",
    #       "name": "Regional"
    #   }
    # ...
    # }
    for index, level in enumerate(levels):
        ou_levels[index] = {"uid": level[0], "name": level[1].replace(" ", "")}

    # read all the org units to transform the org unit hierarchy in the form of id to names and ids
    with open(os.path.join(ORG_UNIT_DIR_JSON, ORG_UNIT_JSON), 'r', encoding="utf-8") as f:
        ous = json.load(f)

        org_units_hierarchy = {}
        # The "path" column is stores org unit hierarchy ids like
        # "/JWOFOcZt7av/FzSzYdX25ch/LtgDnjaiH9b/V1Ora4NbSEE/Nimdjs72wnV"
        # Split the path by forward slash and search the name of the org unit using the ids
        # then store the names and ids in the org_units_hierarchy array.
        for uid, ou_row in ous.items():
            if ou_row['path']['value'].strip().__len__() > 0:
                parent_uids = ou_row['path']['value'].split("/")
                hierarchy = {}

                for index, parent_uid in enumerate(parent_uids):
                    if parent_uid.strip().__len__() > 0:
                        if index - 1 in ou_levels:
                            hierarchy[ou_levels[index - 1]['uid']
                                      ] = {"type": "object", "value": parent_uid}
                            hierarchy[ou_levels[index - 1]['name']] = {
                                "type": "object", "value": ous[parent_uid]["name"]["value"]}

                org_units_hierarchy[uid] = hierarchy

        # insert the hierarchy information into the orgunits json
        for uid, ou_partial_row in org_units_hierarchy.items():
            if uid in ous:
                ous[uid].update(ou_partial_row)
                # add at what level the org unit is
                ous[uid].update(
                    {"level": {"type": "int64", "value": ous[uid]["path"]["value"].count('/')}})

    # rearrange the data to be imported into pandas
    # [{"uid": "abcdef", "name: Ethiopia"}, {"uid":"ab1235", ...}]
    dataset = []

    exclude_fields = ['change', 'path', 'parentid']
    for uid, ou_row in ous.items():
        row = {}
        for col_name, ou_val in ou_row.items():
            if col_name not in exclude_fields:
                row[col_name] = ou_val['value']
        dataset.append(row)

    # Store the org units dataset as csv file
    df = pd.DataFrame(dataset)
    # check if the data frame has a column named 'lastupdated'
    if 'lastupdated' in df.columns:
        # change the data type of lastupdated column to datetime64
        df['lastupdated'] = df['lastupdated'].astype('datetime64')
        # then remove the microseconds from lastupdated column. ClickHouse doesn't support microseconds.
        df['lastupdated'] = df['lastupdated'].apply(
            lambda x: x.replace(microsecond=0))
    df.to_csv(os.path.join(ORG_UNIT_DIR_CSV,
              ORG_UNIT_HIERARCHY_CSV), index=False)


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
            sql="select uid, name, parentid, path, sourceid, change, lastupdated from organisationunit where change = 'insert' or change ='update'",
            unique_keys=['uid'],
            output_file=ORG_UNIT_JSON
        )

        get_org_unit_levels_ids_and_names = PythonOperator(
            task_id='get_org_unit_levels_ids_and_names',
            python_callable=query_and_push,
            provide_context=True,
            op_kwargs={
                'sql': "select uid, name, level from orgunitlevel order by level;",
                'postgres_conn_id': 'postgres',
                'key': 'get_org_unit_levels_id_name'
            }
        )

        generate_and_store_org_unit_hierarchy_in_csv = PythonOperator(
            task_id='generate_and_store_org_unit_hierarchy_in_csv',
            python_callable=generate_org_unit_hierarchy_in_csv
        )

        # convert_org_unit_hierarchy_in_json_2_ch_sql = JSON2CHInsertOperator(
        #     task_id='convert_org_unit_hierarchy_in_json_2_ch_sql',
        #     ch_table_name='orgunit',
        #     input_file=os.path.join(ORG_UNIT_DIR_JSON, ORG_UNIT_HIERARCHY_JSON),
        #     exclude_fields=['change', 'path', 'parentid'],
        #     output_file="orgunit.sql"
        # )

        # import_orgunit_into_ch = ClickHouseMultiSqlOperator(
        #     task_id='import_orgunit_into_ch',
        #     database='core',
        #     clickhouse_conn_id='clickhouse',
        #     sql_file='dags/tmp/ch_sql/orgunit.sql'
        # )

        # import_orgunit_into_ch = ClickHouseOperator(
        #     task_id='import_orgunit_into_ch',
        #     database='core',
        #     sql=(
        #         "INSERT INTO orgunit FROM INFILE '{}' FORMAT CSVWithNames;".format(os.path.join(DAG_FULL_PATH, ORG_UNIT_DIR_CSV, ORG_UNIT_HIERARCHY_CSV))
        #     ),
        #     clickhouse_conn_id='clickhouse',
        # )

        import_orgunit_into_ch = LoadCSV2CHOperator(
            task_id='import_orgunit_into_ch',
            database='core',
            clickhouse_conn_id='clickhouse',
            table_name='orgunit',
            schema={'level': int, 'lastupdated': lambda x: datetime.strptime(
                x, '%Y-%m-%d %H:%M:%S')},
            input_file=os.path.join(ORG_UNIT_DIR_CSV, ORG_UNIT_HIERARCHY_CSV)
        )

        reset_orgunit_in_pgsql = PostgresOperator(
            task_id='reset_orgunit_in_pgsql',
            postgres_conn_id='postgres',
            sql="update organisationunit set change = '' where change = 'update' or change = 'insert'"
        )

        get_org_unit_levels >> generate_orgunit_columns_schema >> import_orgunit_schema_into_ch >> \
            reset_orgunit_level_in_pgsql >> [export_orgunit_from_pgsql_2_json, get_org_unit_levels_ids_and_names] >> \
            generate_and_store_org_unit_hierarchy_in_csv >> import_orgunit_into_ch >> reset_orgunit_in_pgsql

    return group
