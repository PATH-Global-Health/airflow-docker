
import json
import os
from typing import List

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator

class PGSQL2JSONOperator(BaseOperator):
    """
    This operator allows you to dump the result of select query from PostgreSQL to JSON file.
    File structure
    {
        "merged_primary_key_with_dashes_1": {
            "column_name_1": {
                "type": "type",
                "value": "value"
            },
            "column_name_2": {
                "type": "type",
                "value": "value"
            },
            ...
        },
        "merged_primary_key_with_dashes_2": {
           "column_name_1": {
                "type": "type",
                "value": "value"
            },
            "column_name_2": {
                "type": "type",
                "value": "value"
            },
            ...
        },
        ...
    }
    """

    def __init__(self, postgres_conn_id: str, unique_keys: List[str], sql: str, output_file: str, output_dir="dags/tmp/json", **kwargs):
        super().__init__(**kwargs)

        if not postgres_conn_id:
            raise AirflowException('No valid postgres_conn_id supplied.')

        if not unique_keys:
            raise AirflowException('No valid unique keys supplied.')

        if not sql:
            raise AirflowException('No valid sql supplied.')

        if not output_file:
            raise AirflowException('No valid output file name supplied.')

        if not output_dir:
            raise AirflowException('No valid output dir supplied.')

        self.postgres_conn_id = postgres_conn_id
        self.unique_keys = unique_keys
        self.sql = sql
        self.output_file = output_file
        self.output_dir = output_dir

    def change_row_to_json(self, types, columns, row):
       
        cols = {}
        pks = []

        for column in columns:
            if column in self.unique_keys:
                pks.append(row[column])
            cols[column] = {"type":str(types[column]), "value":str(row[column])}

        return '-'.join(pks), cols
        
    def execute(self, context):
        data = {}

        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        pg_df = pg_hook.get_pandas_df(sql=self.sql)
        for index, row in pg_df.iterrows():
            d = self.change_row_to_json(pg_df.dtypes, pg_df.columns, row)
            data[d[0]] = d[1]
            
        # write the dict to file
        with open(os.path.join(self.output_dir, self.output_file), 'w') as f:
            f.write(json.dumps(data))
