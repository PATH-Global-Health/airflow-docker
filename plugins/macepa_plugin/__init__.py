from airflow.plugins_manager import AirflowPlugin
from macepa_plugin.operators.multi_endpoint_operator import MultiEndpointOperator
from macepa_plugin.operators.http_payload_split_operator import HTTPPayloadSplitOperator
from macepa_plugin.operators.dhis2_metadata_download_operator import DHIS2MetadataDownloadOperator
from macepa_plugin.operators.generate_postgresql_operator import GeneratePostgreSQLOperator
from macepa_plugin.operators.generate_postgresql_mn_operator import GeneratePostgreSQLMNOperator
from macepa_plugin.operators.generate_mass_postgresql_operator import GenerateMassPostgreSQLOperator
from macepa_plugin.operators.dhis2_data_download_operator import DHIS2DataDownloadOperator
from macepa_plugin.operators.pgsql_2_ch_upsert_operator import PGSQL2CHUpsertOperator
from macepa_plugin.operators.pgsql_2_ch_insert_operator import PGSQL2CHInsertOperator
from macepa_plugin.operators.clickhouse_multi_sql_operator import ClickHouseMultiSqlOperator
from macepa_plugin.operators.pgsql_2_json_operator import PGSQL2JSONOperator
from macepa_plugin.operators.json_2_ch_insert_operator import JSON2CHInsertOperator
from macepa_plugin.operators.load_csv_2_ch_operator import LoadCSV2CHOperator
from macepa_plugin.operators.generate_ch_table_schema import GenerateCHTableSchema


class MacepaPlugin(AirflowPlugin):
    name = "macepa_plugin"

    operators = [
        MultiEndpointOperator,
        HTTPPayloadSplitOperator,
        DHIS2MetadataDownloadOperator,
        GeneratePostgreSQLOperator,
        GeneratePostgreSQLMNOperator,
        GenerateMassPostgreSQLOperator,
        DHIS2DataDownloadOperator,
        PGSQL2CHUpsertOperator,
        PGSQL2CHInsertOperator,
        PGSQL2JSONOperator,
        ClickHouseMultiSqlOperator,
        JSON2CHInsertOperator,
        LoadCSV2CHOperator,
        GenerateCHTableSchema
    ]

    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
