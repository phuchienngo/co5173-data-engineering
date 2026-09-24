import os
import clickhouse_connect


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"),
        database=os.environ.get("CLICKHOUSE_DB", "default"),
    )


def write_dataframe(df, table):
    get_client().insert_df(table, df)


def truncate_table(table):
    get_client().command(f"TRUNCATE TABLE IF EXISTS {table}")


def read_query(query):
    return get_client().query_df(query)
