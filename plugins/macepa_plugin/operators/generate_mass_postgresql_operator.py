
import json
import os

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException


class GenerateMassPostgreSQLOperator(BaseOperator):
    """
    This operator allows you to generate mass Postgres sql to be imported into the staging database.
    """

    def cast(self, type, value):
        if type == "int" or type == "float":
            return value
        elif type == 'timestamp' or type == 'date':
            return "TO_TIMESTAMP('{}', 'YYYY-MM-DD/THH24:MI:ss.MS')".format(value)
        return "'{}'".format(value)

    def __init__(self, table_name: str, json_key_table_columns_2_map: dict, primary_keys: list, input_tmp_dir="dags/tmp/json/data", output_tmp_dir="dags/tmp/pg_sql/data", **kwargs):
        super().__init__(**kwargs)

        if table_name.strip().__len__() == 0:
            raise AirflowException('No valid table name supplied.')

        if not json_key_table_columns_2_map:
            raise AirflowException(
                'No valid json_key_table_columns_2_map supplied.')

        if not primary_keys:
            raise AirflowException('No valid primary keys supplied.')

        self.input_tmp_dir = input_tmp_dir
        self.output_tmp_dir = output_tmp_dir
        self.table_name = table_name
        self.json_key_table_columns_2_map = json_key_table_columns_2_map
        self.primary_keys = primary_keys

    def execute(self, context):
        # extract the data source we set from xcom
        source = self.xcom_pull(context=context, key='get_hmis_data_source')
        org_unit_ids = self.xcom_pull(context=context, key='get_org_unit_ids')

        for org_unit in org_unit_ids:
            sql = []
            input_json_file = "{}/{}.json".format(
                self.input_tmp_dir, org_unit[0])

            # if the org unit has no data stored locally in json format,
            # continue to the next org unit
            if not os.path.exists(input_json_file):
                continue

            try:
                with open(input_json_file) as f:
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
                                # check if the json_key has a nested column or not e.g. 'categoryCombo': {'id': 'u1uT2mTdt6Q'}}..."
                                if 'nestedColumns' in self.json_key_table_columns_2_map[json_key]:
                                    for nested_key, nested_value in self.json_key_table_columns_2_map[json_key]['nestedColumns'].items():
                                        if nested_key in value:
                                            table_columns.append(
                                                self.json_key_table_columns_2_map[json_key]['nestedColumns'][nested_key]['column'])
                                            # extract the nested value from value and type cast it and store it in the values
                                            values.append(
                                                self.cast(self.json_key_table_columns_2_map[json_key]['nestedColumns'][nested_key]['type'], value[nested_key]))
                                else:
                                    # if it is not nested column, get the column name equivalent of the key from
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

                        # Since we are using the upsert sql, if the insert fails, prepare the columns to be updated
                        update = []
                        for update_column in table_columns:
                            # we update all columns except the primary keys
                            if update_column not in self.primary_keys:
                                update.append(
                                    "{} = EXCLUDED.{}".format(update_column, update_column))

                        # if all columns are primary key, skip the update sql
                        update_columns = "DO NOTHING"
                        if update.__len__() > 0:
                            update_columns = f"DO UPDATE SET {','.join(update)}"

                        # finally merge the columns using comma that we are using in the insert and update query and append it in the sql list
                        sql.append(
                            f"INSERT INTO {self.table_name} ({','.join(table_columns)}) VALUES({','.join(values)}) ON CONFLICT({','.join(self.primary_keys)}) {update_columns};")

                # dump the sql list in a file
                output_sql_file = "{}/{}.sql".format(
                    self.output_tmp_dir, org_unit[0])
                with open(output_sql_file, 'w') as file:
                    file.write("\n".join(sql))

            except Exception as e:
                raise AirflowException(
                    f"An error occurred while converting json to sql with message {e}"
                )
