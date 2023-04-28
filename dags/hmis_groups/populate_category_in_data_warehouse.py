import os
import json

from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

from macepa_plugin import ClickHouseMultiSqlOperator

CH_CATEGORY_TABLE = 'category'
CH_CATEGORY_TABLE_SCHEMA = 'dags/tmp/ch_sql/category_schema.sql'

# category_structure = {
#         "CategoryComboID": {
#             "name": "CategoryComboName",
#             "categories": {
#                     "categoryID": {
#                         "name": "CategoryName",
#                         "previous_name": "PreviousCategoryName",
#                         "change": "insert",
#                         "categoryOptions": {
#                                 "optionID": {
#                                     "name": "OptionName",
#                                 },
#                                 ...
#                             }
#                     },
#                     ...
#                 }
#         },
#         ...
#     }


# def generate_category_structure(ti):
#     category_structure = {}
#     pg_hook = PostgresHook(postgres_conn_id='postgres')
#     pg_df = pg_hook.get_pandas_df('''
#         select
#             cc.uid as CategoryComboID, cc.name as CategoryComboName,
#             dec.uid as DataElementCategoryID, dec.name as DataElementCategoryName, 
#             dec.previous_name as DataElementCategoryPreviousName, dec.change,
#             deco.uid as DataElementCategoryOptionID, deco.name as DataElementCategoryOptionName
#         from dataelementcategory as dec 
#         inner join dataelementcategory_categoryoption as decco on dec.uid=decco.category_id 
#         inner join dataelementcategoryoption as deco on decco.categoryoption_id=deco.uid 
#         inner join dataelementcategory_categorycombo as deccc on deccc.dataelementcategory_id=dec.uid
#         inner join categorycombo as cc on deccc.categorycombo_id =cc.uid;
#     ''')
#     for index, row in pg_df.iterrows():
#         # register the categorycombo if it is not registered 
#         if row[0] not in category_structure:
#             category_structure[row[0]] = {
#                 "name": row[1],
#                 "categories": {}
#             }
        
#         if row[2] not in category_structure[row[0]]['categories']:
#             category_structure[row[0]]['categories'][row[2]] = {
#                 "name": row[3],
#                 "previous_name": row[4],
#                 "change": row[5],
#                 "categoryOptions": {}
#             }
        
#         if row[6] not in category_structure[row[0]]['categories'][row[2]]['categoryOptions']:
#             category_structure[row[0]]['categories'][row[2]]['categoryOptions'][row[6]] = {
#                 "name": row[7]
#             }

#     file_name = "dags/tmp/json/category_structure.json"
#     with open(file_name, 'w') as file:
#         file.write(json.dumps(category_structure))
    
#     ti.xcom_push(key="category_structure_file_name", value="dags/tmp/json/category_structure.json")
    

def generate_category_schema(ti):
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_df = pg_hook.get_pandas_df('''
        select uid, name, previous_name, change 
        from dataelementcategory 
        where change = "insert" or change = "update";
        ''')
    
    sql = []
    previous_field_name = ""

    # iterate over categories under category combo and make the ids and names as columns in category table of clickhouse
    for index, row in pg_df.iterrows():
        if row[3] == "insert":
            if not previous_field_name:
                sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER lastupdated;'.format(CH_CATEGORY_TABLE, row[0]))
                sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(CH_CATEGORY_TABLE, row[1].replace(" ", ""), row[0]))
            else:
                sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(CH_CATEGORY_TABLE, row[0], previous_field_name))
                sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(CH_CATEGORY_TABLE, row[1].replace(" ", ""), row[0]))
            previous_field_name = row[1].replace(" ", "")
        elif row[3] == "update":
            # Users are only allowed to change the names of the categories not their IDs in DHIS2.
            # This is the reason to rename the columns related with name without touching the IDs.
            sql.append('ALTER TABLE {} RENAME COLUMN IF EXISTS {} to {}'.format(CH_CATEGORY_TABLE, category['previous_name'].replace(" ", ""), category['name'].replace(" ", "")))

    with open(os.path.join(CH_CATEGORY_TABLE_SCHEMA), 'w') as f:
        f.write('\n'.join(sql))


def populate_category_in_data_warehouse():
    with TaskGroup('populate_category_in_data_warehouse', tooltip='Populate the category table in the data warehouse') as group:
        # generate_category_structure_in_json = PythonOperator(
        #     task_id='generate_category_structure_in_json',
        #     python_callable=generate_category_structure
        # )

        generate_category_columns_schema = PythonOperator(
            task_id='generate_category_columns_schema',
            python_callable=generate_category_schema
        )

        import_category_schema_into_ch = ClickHouseMultiSqlOperator(
            task_id='import_category_schema_into_ch',
            database='core',
            clickhouse_conn_id='clickhouse',
            sql_file=CH_CATEGORY_TABLE_SCHEMA
        )

        reset_category_level_in_pgsql = PostgresOperator(
            task_id='reset_category_level_in_pgsql',
            postgres_conn_id='postgres',
            sql="update dataelementcategory set change = '' where change = 'update' or change = 'insert'"
        )

        generate_category_columns_schema >> import_category_schema_into_ch >> reset_category_level_in_pgsql

    return group

