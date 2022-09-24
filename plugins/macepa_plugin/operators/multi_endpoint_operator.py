from airflow.exceptions import AirflowException
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.http_hook import HttpHook

class MultiEndpointOperator(SimpleHttpOperator):
    """
    This operator allows you to request as many endpoints at the same time.
    :param endpoints: Your desired endpoints
    :type endpoints: list
    :param endpoints_from_upstream: If you are getting endpoints from upstream task, set it to True
    :type endpoints_from_upstream: bool
    :upstream_task_id: If you getting endpoints from upstream task, set the upstream task id
    :type upstream_task_id: str
    """

    @apply_defaults
    def __init__(self,
                 http_conn_id = None,
                 endpoints=None,
                 endpoints_from_upstream=True,
                 upstream_task_id=None,
                 *args,
                 **kwargs):
        super().__init__(endpoint=None, *args,
                         **kwargs)

        if not http_conn_id:
            raise AirflowException('No valid http_conn_id supplied.')

        if endpoints_from_upstream and upstream_task_id == None:
            raise AirflowException('No valid upstream task_id supplied.')

        if endpoints_from_upstream == False and endpoints == None:
            raise AirflowException('No valid endpoints supplied.')
	
        self.http_conn_id = http_conn_id
        self.endpoints = endpoints
        self.endpoints_from_upstream = endpoints_from_upstream
        self.upstream_task_id = upstream_task_id

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTPs method")

        data = []

        endpoints = self.endpoints
        if self.endpoints_from_upstream:
                endpoints = self.xcom_pull(task_ids=self.upstream_task_id, context=context)

        for ep in endpoints:
            response = http.run(ep,
                                self.data,
                                self.headers,
                                self.extra_options)
            
            data.append({ep:response.content})
     
        return data


