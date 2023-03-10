
import json

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException

from dhis2 import Api, RequestException


class GeneratePostgreSQLOperator(BaseOperator):
    """
    This operator allows you to generate Postgres sql to be imported into the staging database.
    """

    def __init__(self, table_name: str, json_key_table_columns_2_map: dict, primary_keys: list, sql_filename: str, json_file: str, tmp_dir="dags/tmp/pg_sql", **kwargs):
        super().__init__(**kwargs)

        if table_name.strip().__len__() == 0:
            raise AirflowException('No valid table name supplied.')

        if not json_key_table_columns_2_map:
            raise AirflowException(
                'No valid json_key_table_columns_2_map supplied.')

        if not primary_keys:
            raise AirflowException('No valid primary keys supplied.')

        if not sql_filename:
            raise AirflowException('No valid sql file name supplied.')

        if not json_file:
            raise AirflowException('No valid json_file supplied.')

        self.tmp_dir = tmp_dir
        self.table_name = table_name
        self.json_key_table_columns_2_map = json_key_table_columns_2_map
        self.primary_keys = primary_keys
        self.sql_filename = sql_filename
        self.json_file = json_file

    def execute(self, context):
        sql = []
        try:
            with open(self.json_file) as f:
                json_rows = json.load(f)
                # generate sql
                for json_row in json_rows:
                    table_columns = []
                    values = []
                    for json_key, value in json_row.items():
                        if json_key in self.json_key_table_columns_2_map.keys():
                            table_columns.append(json_key)
                            values.append(value)

                    update = []
                    for update_column in table_columns:
                        if update_column != 'uid':
                            update.append(
                                "{} = EXCLUDED.{}".format(update_column))

                    sql.append(
                        f"INSERT INTO {self.table_name} ({','.join(table_columns)}) VALUES({','.join(values)}) ON CONFLICT({','.join(self.primary_keys)}) DO UPDATE SET {','.join(update)};")

            file_name = "{}/{}".format(self.tmp_dir, self.sql_filename)

            with open(file_name, 'w') as file:
                json.dump("\n".join(sql), file)

        except RequestException as e:
            raise AirflowException(
                f"An error occurred while converting json to sql with message {e}"
            )
