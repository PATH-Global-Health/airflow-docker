from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
import json
import logging


class DHIS2MetadataDownloadOperator(PythonOperator):
    """
    This operator allows you to download metadata from DHIS2.
    :param payload_size: Your desired payload size
    :type endpoints: int
    """

    @apply_defaults
    def __init__(self,
                 endpoint=None,
                 http_conn_id=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.endpoint = endpoint
        self.http_conn_id = http_conn_id

    def execute(self, context):
        connection = BaseHook.get_connection(self.http_conn_id)
        self.log.info(connection.username)

        # self.log.info("Calling HTTPs method")#	logger.log("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")#        logger.log(self.data)
        # #logging.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        # #logging.info(type(self.data))
        # d = json.loads(self.data.replace("\'", "\""))
        # payload = d["dataValues"]
        # #logging.info(self.data)
        # for i in range(0, len(payload), self.payload_size):
        #     response = http.run(self.endpoint,
        #                         json.dumps({"dataValues": payload[i:i + self.payload_size]}),
        #                         self.headers,
        #                         self.extra_options)
        #     #logging.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        #     #logging.info(json.dumps({"dataValues": payload[i:i + self.payload_size]}))
        #     if self.response_check:
        #         if not self.response_check(response):
        #             print(response)
        #             raise AirflowException("Response check returned False.")
