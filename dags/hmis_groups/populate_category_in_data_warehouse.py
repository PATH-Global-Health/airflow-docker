import os
import json
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

from macepa_plugin import ClickHouseMultiSqlOperator, GenerateCHTableSchema

CH_CATEGORY_TABLE_SCHEMA = 'dags/tmp/ch_sql/categorySchema.sql'
CATEGORY_METADATA_JSON_FILE = 'dags/tmp/json/categoryMetadata.json'
CATEGORY_METADATA_SQL_FILE = 'dags/tmp/ch_sql/categoryMetadata.sql'

# Data structure for category metadata
# {
#     "categoryoptioncomboid_1": {
#         "name": "categoryoptioncomboname",
#         "source": "c1x82kksj"
#         "options": {
#               "categoryid":"categoryoption_id",
#               "categoryname":"categoryoption_name",
#         }
#         ...
#     },
#     "categoryoptioncomboid_2": {
#         "name": "categoryoptioncomboname",
#         "source": "c1x82kksj"
#         "options": {
#               "categoryid":"categoryoption_id",
#               "categoryname":"categoryoption_name",
#         }
#         ...
#     }
# }


def convert_category_metadata(ti):
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_df = pg_hook.get_pandas_df("""
       select coc.uid, coc.name,de_c.uid, de_c.name, deco.uid, deco.name, deco.source_id
       from categoryoptioncombo as coc 
       inner join categoryoptioncombo_categoryoptions as cocco on coc.uid = cocco.category_option_combo_id 
       inner join dataelementcategoryoption as deco on cocco.category_option_id = deco.uid
       inner join dataelementcategory_categoryoption as decco on decco.categoryoption_id = deco.uid 
       inner join dataelementcategory as de_c on de_c.uid = decco.category_id
       where deco.change = 'insert' or  deco.change = 'update'
       order by coc.uid ;""")
    data = {}

    # row[0] - category option combo uid
    # row[1] - category option combo name e.g. Male, 0-4 years, control
    # row[2] - data element category uid
    # row[3] - data element category name e.g. Sex
    # row[4] - data element category option uid
    # row[5] - data element category option name e.g. Male
    # row[6] - data source id
    for index, row in pg_df.iterrows():
        if row[0] not in data:
            data[row[0]] = {"name": row[1], "source": row[6], "options": {}}

        data[row[0]]["options"][row[2]] = row[4]
        data[row[0]]["options"][row[3]] = row[5]

    with open(CATEGORY_METADATA_JSON_FILE, 'w') as file:
        file.write(json.dumps(data))


def convert_category_metadata_in_json_2_sql(ti):

    with open(CATEGORY_METADATA_JSON_FILE, 'r', encoding="utf-8") as f:
        categories = json.load(f)
        sql = []

        for category_key, category_value in categories.items():
            values = []
            cols = []

            cols.append('categoryoptioncomboid')
            values.append("'{}'".format(category_key))

            cols.append('categoryoptioncomboname')
            values.append("'{}'".format(category_value['name']))

            cols.append('source_id')
            values.append("'{}'".format(category_value['source']))

            cols.append('lastupdated')
            values.append("'{}'".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

            for category_option_key, category_option_value in category_value['options'].items():
                # remove space in column name
                cols.append(category_option_key.replace(' ', ''))
                values.append("'{}'".format(category_option_value))

            sql.append("INSERT INTO category ({}) VALUES ({})".format(
                ', '.join(cols), ', '.join(values)))

    with open(CATEGORY_METADATA_SQL_FILE, 'w') as f:
        f.write('\n'.join(sql))


def populate_category_in_data_warehouse():
    with TaskGroup('populate_category_in_data_warehouse', tooltip='Populate the category table in the data warehouse') as group:

        generate_category_columns_schema = GenerateCHTableSchema(
            task_id='generate_category_columns_schema',
            ch_table_name='category',
            postgres_conn_id='postgres',
            pg_table_name='dataelementcategory',
            output_file='categorySchema.sql'
        )

        import_category_schema_into_ch = ClickHouseMultiSqlOperator(
            task_id='import_category_schema_into_ch',
            database='core',
            clickhouse_conn_id='clickhouse',
            sql_file=CH_CATEGORY_TABLE_SCHEMA
        )

        reset_category_in_pgsql = PostgresOperator(
            task_id='reset_category_in_pgsql',
            postgres_conn_id='postgres',
            sql="update dataelementcategory set change = '' where change = 'update' or change = 'insert'"
        )

        convert_category_metadata_to_json = PythonOperator(
            task_id='convert_category_metadata_to_json',
            python_callable=convert_category_metadata
        )

        convert_category_metadata_in_json_to_sql = PythonOperator(
            task_id='convert_category_metadata_in_json_to_sql',
            python_callable=convert_category_metadata_in_json_2_sql
        )

        import_category_metadata_into_clickhouse = ClickHouseMultiSqlOperator(
            task_id='import_category_metadata_into_clickhouse',
            database='core',
            clickhouse_conn_id='clickhouse',
            sql_file=CATEGORY_METADATA_SQL_FILE
        )

        reset_category_options_in_pgsql = PostgresOperator(
            task_id='reset_category_options_in_pgsql',
            postgres_conn_id='postgres',
            sql="update dataelementcategoryoption set change = '' where change = 'update' or change = 'insert'"
        )

        generate_category_columns_schema >> import_category_schema_into_ch >> reset_category_in_pgsql >> \
            convert_category_metadata_to_json >> convert_category_metadata_in_json_to_sql >> \
            import_category_metadata_into_clickhouse >> reset_category_options_in_pgsql

    return group
