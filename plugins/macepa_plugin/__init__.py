from airflow.plugins_manager import AirflowPlugin
from macepa_plugin.operators.multi_endpoint_operator import MultiEndpointOperator
from macepa_plugin.operators.http_payload_split_operator import  HTTPPayloadSplitOperator

class MacepaPlugin(AirflowPlugin):
	name="macepa_plugin"

	operators = [
		MultiEndpointOperator,
		HTTPPayloadSplitOperator
	]

	hooks = []
	executors = []
	macros = []
	admin_views = []
	flask_blueprints = []
	menu_links = []
