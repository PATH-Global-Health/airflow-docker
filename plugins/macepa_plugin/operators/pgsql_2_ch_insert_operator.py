
import json
import os
from typing import List

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook


class PGSQL2CHInsertOperator(BaseOperator):
    """
    This operator allows you to generate ClickHouse insert sql by reading data from PostgreSQL database.
    """

    def cast(self, type, value):
        if type == "int64" or type == "float64":
            return value
        elif type == 'datetime64' or type == 'datetime64[ns]':
            return "cast('{}', 'DateTime64')".format(value)
        return "'{}'".format(value)

    def __init__(self, postgres_conn_id: str, ch_table_name: str, sql: str, exclude_fields: List[str], output_file: str, output_dir="dags/tmp/ch_sql/data", **kwargs):
        super().__init__(**kwargs)

        if not postgres_conn_id:
            raise AirflowException('No valid postgres_conn_id supplied.')

        if not ch_table_name:
            raise AirflowException(
                'No valid table name for ClickHouse "ch_table_name" supplied.')

        if not sql:
            raise AirflowException('No valid sql supplied.')

        if not output_file:
            raise AirflowException('No valid output file name supplied.')

        if not output_dir:
            raise AirflowException('No valid output dir supplied.')

        self.postgres_conn_id = postgres_conn_id
        self.ch_table_name = ch_table_name
        self.sql = sql
        self.exclude_fields = exclude_fields
        self.output_file = output_file
        self.output_dir = output_dir

    def generate_insert(self, types, columns, row):
        sql = "INSERT INTO {} ({}) VALUES ({})"
        values = []
        cols = []

        for column in columns:
            if column not in self.exclude_fields:
                cols.append(column)
                values.append(self.cast(types[column], row[column]))

        return sql.format(self.ch_table_name, ', '.join(cols), ', '.join(values))

    def execute(self, context):
        sql = []
        # extract the data source we set from xcom
        source = self.xcom_pull(context=context, key='get_hmis_data_source')

        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        pg_df = pg_hook.get_pandas_df(sql=self.sql)
        for index, row in pg_df.iterrows():
            sql.append(self.generate_insert(pg_df.dtypes, pg_df.columns, row))

        # write the sql to file
        with open(os.path.join(self.output_dir, self.output_file), 'w') as f:
            f.write('\n'.join(sql))
