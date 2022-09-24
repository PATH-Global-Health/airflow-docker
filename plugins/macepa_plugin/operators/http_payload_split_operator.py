from airflow.exceptions import AirflowException
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.http_hook import HttpHook
import json
import logging

class HTTPPayloadSplitOperator(SimpleHttpOperator):
    """
    This operator allows you to request as many endpoints at the same time.
    :param payload_size: Your desired payload size
    :type endpoints: int
    """

    @apply_defaults
    def __init__(self,
                 payload_size=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.payload_size = payload_size

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTPs method")#	logger.log("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")#        logger.log(self.data)
        #logging.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        #logging.info(type(self.data))
        d = json.loads(self.data.replace("\'", "\""))
        payload = d["dataValues"]
        #logging.info(self.data) 
        for i in range(0, len(payload), self.payload_size):
            response = http.run(self.endpoint,
                                json.dumps({"dataValues": payload[i:i + self.payload_size]}),
                                self.headers,
                                self.extra_options)
            #logging.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            #logging.info(json.dumps({"dataValues": payload[i:i + self.payload_size]}))
            if self.response_check:
                if not self.response_check(response):
                    print(response)
                    raise AirflowException("Response check returned False.")
