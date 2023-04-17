from airflow.hooks.postgres_hook import PostgresHook


def query_and_push(ti, key, sql, postgres_conn_id):
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    records = pg_hook.get_records(sql=sql)
    ti.xcom_push(key=key, value=records)
