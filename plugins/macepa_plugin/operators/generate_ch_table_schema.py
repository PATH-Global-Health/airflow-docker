
import json
import os
from typing import List

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook


class GenerateCHTableSchema(BaseOperator):
    """
    This operator allows you to generate ClickHouse table schema.
    """

    def cast(self, type, value):
        if type == "int64" or type == "float64":
            return str(value)
        elif type == 'datetime64' or type == 'datetime64[ns]':
            return "cast('{}', 'DateTime64')".format(value)
        return "'{}'".format(value)

    def __init__(self, ch_table_name: str, postgres_conn_id: str, pg_table_name: str, output_file: str, output_dir="dags/tmp/ch_sql", **kwargs):
        super().__init__(**kwargs)

        if not ch_table_name:
            raise AirflowException(
                'No valid table name for ClickHouse "ch_table_name" supplied.')

        if not output_file:
            raise AirflowException('No valid output file name supplied.')

        if not output_dir:
            raise AirflowException('No valid output dir supplied.')

        if not pg_table_name:
            raise AirflowException('No valid pg_table_name supplied.')

        if not postgres_conn_id:
            raise AirflowException('No valid postgres_conn_id supplied.')

        self.ch_table_name = ch_table_name
        self.output_file = output_file
        self.output_dir = output_dir
        self.postgres_conn_id = postgres_conn_id
        self._sql = "select uid, name, previous_name, change from {} where change = 'insert' or change = 'update';".format(
            pg_table_name)

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        pg_df = pg_hook.get_pandas_df(self._sql)

        sql = []
        previous_field_name = ""

        # row[0] - uid
        # row[1] - name
        # row[2] - previous_name
        # row[3] - change
        for index, row in pg_df.iterrows():
            if row[3] == "insert":
                if not previous_field_name:
                    sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER lastupdated;'.format(
                        self.ch_table_name, row[0]))
                    sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(
                        self.ch_table_name, row[1].replace(" ", ""), row[0]))
                else:
                    sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(
                        self.ch_table_name, row[0], previous_field_name))
                    sql.append('ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} String AFTER {};'.format(
                        self.ch_table_name, row[1].replace(" ", ""), row[0]))
                # remove space in column name
                previous_field_name = row[1].replace(" ", "")
            elif row[3] == "update":
                # Users are only allowed to change the names of categories, data elements... not their IDs in DHIS2.
                # This is the reason to rename the columns related with name without touching the IDs.
                sql.append('ALTER TABLE {} RENAME COLUMN IF EXISTS {} to {}'.format(
                    self.ch_table_name, category['previous_name'].replace(" ", ""), category['name'].replace(" ", "")))

        with open(os.path.join(self.output_dir, self.output_file), 'w') as f:
            f.write('\n'.join(sql))
