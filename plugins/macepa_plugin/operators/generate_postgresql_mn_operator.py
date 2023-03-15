
import json

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException


class GeneratePostgreSQLMNOperator(BaseOperator):
    """
    This operator allows you to generate Postgres sql to tables with many to many relation to be imported into the staging database.
    """

    def cast(self, type, value):
        if type == "int" or type == "float":
            return value
        elif type == 'timestamp' or type == 'date':
            return "TO_TIMESTAMP('{}', 'YYYY-MM-DD/THH24:MI:ss.MS')".format(value)
        return "'{}'".format(value)

    def __init__(self, table_name: str, json_key_table_columns_2_map: dict, primary_keys: list, sql_filename: str, json_file: str,
                 target_list_key_for_mn: str, mn_json_key_table_columns_2_map: dict, tmp_dir="dags/tmp/pg_sql", **kwargs):
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

        if not json_file:
            raise AirflowException('No valid json_file supplied.')

        if not target_list_key_for_mn:
            raise AirflowException('No valid target_list_key_for_mn supplied.')

        if not mn_json_key_table_columns_2_map:
            raise AirflowException(
                'No valid mn_json_key_table_columns_2_map supplied.')

        self.tmp_dir = tmp_dir
        self.table_name = table_name
        self.json_key_table_columns_2_map = json_key_table_columns_2_map
        self.primary_keys = primary_keys
        self.sql_filename = sql_filename
        self.json_file = json_file
        self.target_list_key_for_mn = target_list_key_for_mn
        self.mn_json_key_table_columns_2_map = mn_json_key_table_columns_2_map

    def execute(self, context):
        # extract the data source we set from xcom
        source = self.xcom_pull(context=context, key='get_hmis_data_source')
        sql = []
        try:
            with open(self.json_file) as f:
                json_rows = json.load(f)
                # generate sql
                # for every json row in the metadata dump
                for json_row in json_rows:
                    table_columns = []
                    values = []
                    # extract the key value pair like the uid, code, name ...
                    for json_key, value in json_row.items():
                        # check if the extracted key exists in the json key to table column map variable
                        if json_key in self.json_key_table_columns_2_map.keys():
                            # if it exists, get the column name equivalent of the key from
                            # json_key_table_columns_2_map dictionary and store it in table_columns list
                            table_columns.append(
                                self.json_key_table_columns_2_map[json_key]['column'])
                            # type cast the value and store it in the values
                            values.append(
                                self.cast(self.json_key_table_columns_2_map[json_key]['type'], value))

                    # set the data source foreign key
                    if source:
                        table_columns.append(source['id'])
                        values.append("MD5('{}')".format(source['url']))

                    # read all data from a list provided as target_list_key_for_MN for the many to many relationship
                    # check to see if the current row has the target list key
                    if self.target_list_key_for_mn in json_row.keys():
                        # for every row in the target list
                        for mn_row in json_row[self.target_list_key_for_mn]:
                            table_mn_columns = []
                            mn_values = []
                            # extract the key value pair like the id ...
                            for mn_row_key, mn_row_value in mn_row.items():
                                # check if the extracted key exists in the json key to table column map variable
                                if mn_row_key in self.mn_json_key_table_columns_2_map.keys():
                                    # if it exists, get the column name equivalent of the key from
                                    # mn_json_key_table_columns_2_map dictionary and store it in table_columns list
                                    table_mn_columns.append(
                                        self.mn_json_key_table_columns_2_map[mn_row_key]['column'])
                                    # type cast the value and store it in the values
                                    mn_values.append(
                                        self.cast(self.mn_json_key_table_columns_2_map[mn_row_key]['type'], mn_row_value))

                            # Since we are using the upsert sql, if the insert fails, prepare the columns to be updated
                            update = []
                            for update_column in table_columns:
                                # we update all columns except the primary keys
                                if update_column not in self.primary_keys:
                                    update.append(
                                        "{} = EXCLUDED.{}".format(update_column, update_column))

                            # for the many to many relationship
                            for update_column in table_mn_columns:
                                # we update all columns except the primary keys
                                if update_column not in self.primary_keys:
                                    update.append(
                                        "{} = EXCLUDED.{}".format(update_column, update_column))

                            # if all columns are primary key, skip the update sql
                            update_columns = ""
                            if update.__len__() > 0:
                                update_columns = f" DO UPDATE SET {','.join(update)}"

                            # finally merge the columns using comma that we are using in the insert and update query and append it in the sql list
                            merged_table_columns = [
                                *table_columns, *table_mn_columns]
                            merged_values = [*values, *mn_values]

                            sql.append(
                                f"INSERT INTO {self.table_name} ({','.join(merged_table_columns)}) VALUES({','.join(merged_values)}) ON CONFLICT({','.join(self.primary_keys)}){update_columns};")

            # dump the sql list in a file
            file_name = "{}/{}".format(self.tmp_dir, self.sql_filename)
            with open(file_name, 'w') as file:
                file.write("\n".join(sql))

        except Exception as e:
            raise AirflowException(
                f"An error occurred while converting json to sql with message {e}"
            )
