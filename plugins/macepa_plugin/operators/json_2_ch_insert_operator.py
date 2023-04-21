
import json
import os
from typing import List

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

class JSON2CHInsertOperator(BaseOperator):
    """
    This operator allows you to generate ClickHouse insert sql by reading data from JSON dataset.
    """

    def cast(self, type, value):
        if type == "int64" or type == "float64":
            return value
        elif type == 'datetime64':
            return "cast('{}', 'DateTime64')".format(value)
        return "'{}'".format(value)

    def __init__(self, ch_table_name:str, exclude_fields: List[str], input_file:str, output_file: str, output_dir="dags/tmp/ch_sql", **kwargs):
        super().__init__(**kwargs)

        if not ch_table_name:
            raise AirflowException('No valid table name for ClickHouse "ch_table_name" supplied.')

        if not output_file:
            raise AirflowException('No valid output file name supplied.')

        if not output_dir:
            raise AirflowException('No valid output dir supplied.')

        if not input_file:
            raise AirflowException('No valid input file supplied.')

        self.ch_table_name = ch_table_name
        self.exclude_fields = exclude_fields
        self.output_file = output_file
        self.output_dir = output_dir
        self.input_file = input_file

    def generate_insert(self, row):
        sql = "INSERT INTO {} ({}) VALUES ({})"
        values = []
        cols = []

        for column, ou in row.items():
            if column not in self.exclude_fields:
                cols.append(column)
                values.append(self.cast(ou['type'],ou['value']))

        return sql.format(self.ch_table_name, ', '.join(cols), ', '.join(values))

    def execute(self, context):
        sql = []
        with open(os.path.join(self.input_file), 'r', encoding="utf-8") as f:
            ous = json.load(f)
            for ou_uid, ou_row in ous.items():
                sql.append( self.generate_insert(ou_row))
        
        # write the sql to file
        with open(os.path.join(self.output_dir, self.output_file), 'w') as f:
            f.write('\n'.join(sql))
