
import json
import os
import csv
from enum import Enum

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook


class LoadCSV2CHOperator(BaseOperator):
    """
    This operator loads data from csv to ClickHouse.
    """

    def __init__(self, clickhouse_conn_id: str, database:str, table_name:str, input_file: str, schema={}, **kwargs):
        super().__init__(**kwargs)

        if not clickhouse_conn_id:
            raise AirflowException('No valid clickhouse_conn_id supplied.')

        if not database:
            raise AirflowException('No valid database supplied.')

        if not table_name:
            raise AirflowException('No valid table name supplied.')

        if not input_file:
            raise AirflowException('No valid input file supplied.')

        self.clickhouse_conn_id = clickhouse_conn_id
        self.database = database
        self.table_name = table_name
        self.schema = schema
        self.input_file = input_file
        self._bypass= lambda x: x

    def _row_reader(self):
        with open(self.input_file, "r", encoding="utf-8") as data:
            for row in csv.DictReader(data):
                typed_row = {}
                for k, v in row.items():
                    # we are type casting every value with the type defined in the schema
                    typed_row[k] = self.schema.get(k, self._bypass)(v)
                print(typed_row)
                yield typed_row 

    def execute(self, context):
        hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id, database=self.database)
        client = hook.get_conn()

        client.execute("INSERT INTO {} VALUES".format(self.table_name), (row for row in self._row_reader()))



