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


def generate_category_structure(ti):
    category_structure = {}
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_df = pg_hook.get_pandas_df('''
        select
            cc.uid as CategoryComboID, cc.name as CategoryComboName,
            dec.uid as DataElementCategoryID, dec.name as DataElementCategoryName, 
            dec.previous_name as DataElementCategoryPreviousName, dec.change,
            deco.uid as DataElementCategoryOptionID, deco.name as DataElementCategoryOptionName
        from dataelementcategory as dec 
        inner join dataelementcategory_categoryoption as decco on dec.uid=decco.category_id 
        inner join dataelementcategoryoption as deco on decco.categoryoption_id=deco.uid 
        inner join dataelementcategory_categorycombo as deccc on deccc.dataelementcategory_id=dec.uid
        inner join categorycombo as cc on deccc.categorycombo_id =cc.uid;
    ''')
    for index, row in pg_df.iterrows():
        # register the categorycombo if it is not registered 
        if row[0] not in category_structure:
            category_structure[row[0]] = {
                "name": row[1],
                "categories": {}
            }
        
        if row[2] not in category_structure[row[0]]['categories']:
            category_structure[row[0]]['categories'][row[2]] = {
                "name": row[3],
                "previous_name": row[4],
                "change": row[5],
                "categoryOptions": {}
            }
        
        if row[6] not in category_structure[row[0]]['categories'][row[2]]['categoryOptions']:
            category_structure[row[0]]['categories'][row[2]]['categoryOptions'][row[6]] = {
                "name": row[7]
            }

    file_name = "dags/tmp/json/category_structure.json"
    with open(file_name, 'w') as file:
        file.write(json.dumps(category_structure))
    
    ti.xcom_push(key="category_structure_file_name", value="dags/tmp/json/category_structure.json")
    

def generate_category_schema(ti):
    category_structure_file_name = ti.xcom_pull(key='category_structure_file_name')
    sql = []
    previous_field_name = ""

    # read category structure json file
    with open(category_structure_file_name, 'r', encoding="utf-8") as f:
        category_structure = json.load(f)

    # iterate over categories under category combo and make the ids and names as columns in category table of clickhouse
    for cc_id, category_combo in category_structure.items():
        for c_id, category in category_combo['categories'].items():
            if category['change'] == "insert":
                if not previous_field_name:
                    sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER lastupdated;'.format(CH_CATEGORY_TABLE, c_id))
                    sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(CH_CATEGORY_TABLE, category['name'].replace(" ", ""), c_id))
                else:
                    sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(CH_CATEGORY_TABLE, c_id, previous_field_name))
                    sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(CH_CATEGORY_TABLE, category['name'].replace(" ", ""), c_id))
                previous_field_name = category['name'].replace(" ", "")
            elif category['change'] == "update":
                # Users are only allowed to change the names of the categories not their IDs in DHIS2.
                # This is the reason to rename the columns related with name without touching the IDs.
                sql.append('ALTER TABLE {} RENAME COLUMN IF EXISTS {} to {}'.format(CH_CATEGORY_TABLE, category['previous_name'].replace(" ", ""), category['name'].replace(" ", "")))

    with open(os.path.join(CH_CATEGORY_TABLE_SCHEMA), 'w') as f:
        f.write('\n'.join(sql))


def populate_category_in_data_warehouse():
    with TaskGroup('populate_category_in_data_warehouse', tooltip='Populate the category table in the data warehouse') as group:
        generate_category_structure_in_json = PythonOperator(
            task_id='generate_category_structure_in_json',
            python_callable=generate_category_structure
        )

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

        generate_category_structure_in_json >> generate_category_columns_schema >> import_category_schema_into_ch

    return group

