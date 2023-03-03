
import logging
import json

from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

from dhis2 import Api, RequestException


class DHIS2MetadataDownloadOperator(BaseOperator):
    """
    This operator allows you to download metadata from DHIS2.
    """

    # @apply_defaults
    def __init__(self, endpoint=None, http_conn_id=None, fields=":all", **kwargs):
        super().__init__(**kwargs)

        self.endpoint = endpoint
        self.fields = fields

        connection = BaseHook.get_connection(http_conn_id)
        url = connection.host
        username = connection.login
        password = connection.get_password()

        self._api = Api(url, username, password)

    def execute(self, context):
        try:
            response_data = self._api.get_paged(
                self.endpoint, params={"fields": self.fields}, merge=True
            )
        except RequestException as e:  # Always handle exceptions
            raise AirflowException(
                # Prefer f-strings to concatenation / string interpolation
                f"An error occurred while fetching DHIS2 data (URL: {e.url}, status code: {e.code})"
            )

        file_name = "dags/tmp/{}.json".format(self.endpoint)
        xcom_key = "DHIS2MetadataDownloadOperator_{}".format(self.endpoint)
        with open(file_name, 'w') as file:
            json.dump(response_data[self.endpoint], file)

            self.xcom_push(context=context, key=xcom_key, value=file_name)
