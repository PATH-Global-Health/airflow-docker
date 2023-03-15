from airflow.plugins_manager import AirflowPlugin
from macepa_plugin.operators.multi_endpoint_operator import MultiEndpointOperator
from macepa_plugin.operators.http_payload_split_operator import HTTPPayloadSplitOperator
from macepa_plugin.operators.dhis2_metadata_download_operator import DHIS2MetadataDownloadOperator
from macepa_plugin.operators.generate_postgresql_operator import GeneratePostgreSQLOperator
from macepa_plugin.operators.generate_postgresql_mn_operator import GeneratePostgreSQLMNOperator


class MacepaPlugin(AirflowPlugin):
    name = "macepa_plugin"

    operators = [
        MultiEndpointOperator,
        HTTPPayloadSplitOperator,
        DHIS2MetadataDownloadOperator,
        GeneratePostgreSQLOperator,
        GeneratePostgreSQLMNOperator
    ]

    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
