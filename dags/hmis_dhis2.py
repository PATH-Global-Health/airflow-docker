import os
from datetime import timedelta
import shutil
import itertools

import airflow
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook

from hmis_groups.process_org_units_metadata import process_org_units_metadata
from hmis_groups.process_categories_metadata import process_categories_metadata
from hmis_groups.process_category_combos_metadata import process_category_combos_metadata
from hmis_groups.process_category_options_metadata import process_category_options_metadata
from hmis_groups.process_category_option_combos_metadata import process_category_option_combos_metadata
from hmis_groups.process_data_elements_metadata import process_data_elements_metadata
from hmis_groups.process_data_element_groups_metadata import process_data_element_groups_metadata
from hmis_groups.process_orgunit_level_metadata import process_orgunit_level_metadata
from hmis_groups.process_data import process_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['bserda@path.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(days=15),
}

# ti - task instance


def _set_data_source(ti, http_conn_id):
    connection = BaseHook.get_connection(http_conn_id)
    url = connection.host
    ti.xcom_push(key="get_hmis_data_source", value={
                 'id': 'source_id', 'url': url})


def _clean_tmp_dir(main_dir, sub_dir):
    # delete dir_path
    shutil.rmtree(main_dir)

    # create the the main and sub directories
    for sd in sub_dir:
        try:
            os.makedirs(os.path.join(main_dir, sd))
        except OSError:
            pass


with DAG('HMIS-DHIS2',  default_args=default_args,
         description='''A pipeline for reading data from HMIS/DHIS2 and storing it in a
         PostgreSQL staging database, after which the data is transformed and stored in the ClickHouse.''',
         schedule='0 19 * * *', catchup=False) as dag:

    clean_tmp_dir = PythonOperator(
        task_id='clean_tmp_dir',
        python_callable=_clean_tmp_dir,
        op_kwargs={"main_dir": 'dags/tmp', "sub_dir": [
            'ch_sql', 'json', 'pg_sql', 'ch_sql/data', 'json/data', 'pg_sql/data']},
    )

    create_staging_tables = PostgresOperator(
        task_id='create_staging_tables',
        postgres_conn_id='postgres',
        sql="sql/pg_create_tables.sql"
    )

    populate_data_source_tables = PostgresOperator(
        task_id='populate_data_source_tables',
        postgres_conn_id='postgres',
        sql="sql/pg_data_source_table.sql"
    )

    set_data_source = PythonOperator(
        task_id='set_data_source',
        python_callable=_set_data_source,
        op_kwargs={"http_conn_id": 'hmis_dhis2_api'},
    )

    import_category_category_combos = PostgresOperator(
        task_id='import_category_category_combos',
        postgres_conn_id='postgres',
        sql="tmp/pg_sql/category-CategoryCombos.sql"
    )

    import_category_category_options = PostgresOperator(
        task_id='import_category_category_options',
        postgres_conn_id='postgres',
        sql="tmp/pg_sql/category-CategoryOptions.sql"
    )

    process_hmis_org_units_metadata = process_org_units_metadata()
    process_hmis_categories_metadata = process_categories_metadata()
    process_hmis_category_combos_metadata = process_category_combos_metadata()
    process_hmis_category_options_metadata = process_category_options_metadata()
    process_hmis_category_option_combos_metadata = process_category_option_combos_metadata()
    process_hmis_data_elements_metadata = process_data_elements_metadata()
    process_hmis_data_element_groups_metadata = process_data_element_groups_metadata()
    process_hmis_data = process_data()

    clean_tmp_dir >> create_staging_tables >> populate_data_source_tables >> set_data_source >> \
        [
            process_hmis_org_units_metadata,
            process_hmis_categories_metadata,
            process_hmis_category_combos_metadata,
            process_hmis_category_options_metadata,
            process_hmis_data_element_groups_metadata
        ] >> import_category_category_combos >> import_category_category_options >> \
        [process_hmis_category_option_combos_metadata,
            process_hmis_data_elements_metadata] >> process_hmis_data
