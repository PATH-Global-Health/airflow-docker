
import logging
import json

from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.models import Variable

from dhis2 import Api, RequestException


class DHIS2DataDownloadOperator(BaseOperator):
    """
    This operator allows you to download data from DHIS2.
    """

    def __init__(self, http_conn_id: str, tmp_dir="dags/tmp/json/data", **kwargs):
        super().__init__(**kwargs)

        if not http_conn_id:
            raise AirflowException('No valid http_conn_id supplied.')

        self.http_conn_id = http_conn_id

        self.tmp_dir = tmp_dir
        self._endpoint = "dataValueSets.json"
        self._data_value_tag = "dataValues"

        # define hmis_data_element_group variable in Airflow with value
        # {'dataElementGroup':'qTQ2PV2olmb',dataElementGroup:'...'}
        self._data_element_group = Variable.get(
            "hmis_data_element_group", deserialize_json=True)

        connection = BaseHook.get_connection(http_conn_id)
        url = connection.host
        username = connection.login
        password = connection.get_password()

        self._api = Api(url, username, password)

    def execute(self, context):
        last_updated = self.xcom_pull(context=context,
                                      key='get_hmis_last_updated')
        org_unit_ids = self.xcom_pull(context=context,
                                      key='get_org_unit_ids')

        for org_unit in org_unit_ids:
            try:
                response_data = self._api.get(
                    self._endpoint, params={
                        "orgUnit": org_unit[0],
                        "lastUpdated": last_updated,
                        **self._data_element_group
                    }
                )

            except RequestException as e:  # Always handle exceptions
                raise AirflowException(
                    # Prefer f-strings to concatenation / string interpolation
                    f"An error occurred while fetching DHIS2 data (URL: {e.url}, status code: {e.code})"
                )

            try:
                # converting into json will fail if the response is empty
                data_values = response_data.json()
                if data_values:
                    file_name = "{}/{}.json".format(self.tmp_dir, org_unit[0])
                    with open(file_name, 'w') as file:
                        if self._data_value_tag in data_values:
                            json.dump(data_values[self._data_value_tag], file)
            except json.decoder.JSONDecodeError:
                pass
