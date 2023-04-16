from typing import Any, Dict, Optional
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook


class ClickHouseMultiSqlOperator(BaseOperator):
    """
    This operator allows you to run multiple sql statements from sql file against ClickHouse.
    """

    def __init__(self, sql_file: str, clickhouse_conn_id: str, database: str, parameters: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(**kwargs)

        if not sql_file:
            raise AirflowException('No valid sql_file supplied.')

        if not clickhouse_conn_id:
            raise AirflowException('No valid clickhouse_conn_id supplied.')

        if not database:
            raise AirflowException('No valid database supplied.')

        self.sql_file = sql_file
        self.clickhouse_conn_id = clickhouse_conn_id
        self.database = database
        self._parameters = parameters

    def execute(self, context):

        hook = ClickHouseHook(
            clickhouse_conn_id=self.clickhouse_conn_id,
            database=self.database,
        )

        with open(self.sql_file) as f:
            for sql in f:
                hook.run(sql, self._parameters)
