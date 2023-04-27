import json

from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator

# category_structure = {
#         "CategoryComboID": {
#             "name": "CategoryComboName",
#             "categories": {
#                     "categoryID": {
#                         "name": "CategoryName",
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
                "categoryOptions": {}
            }
        
        if row[4] not in category_structure[row[0]]['categories'][row[2]]['categoryOptions']:
            category_structure[row[0]]['categories'][row[2]]['categoryOptions'][row[4]] = {
                "name": row[5]
            }

    file_name = "dags/tmp/json/category_structure.json"
    with open(file_name, 'w') as file:
        file.write(json.dumps(category_structure))
    
    ti.xcom_push(key="category_structure_file_name", value="dags/tmp/json/category_structure.json")
    

def populate_category_in_data_warehouse():
    with TaskGroup('populate_category_in_data_warehouse', tooltip='Populate the category table in the data warehouse') as group:
        generate_category_structure_in_json = PythonOperator(
            task_id='generate_category_structure_in_json',
            python_callable=generate_category_structure
        )


    return group

