import os
import clickhouse_connect


def connection_settings():
    return {
        "host": os.environ.get("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        "username": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"),
        "database": os.environ.get("CLICKHOUSE_DB", "default"),
    }


def get_client(**overrides):
    return clickhouse_connect.get_client(**{**connection_settings(), **overrides})


def write_dataframe(df, table):
    get_client().insert_df(table, df)


def write_partitions(df, table, database=None, batch_rows=50_000):
    # Parallel writer: each Spark partition inserts its own rows from the
    # executor, so nothing is collected on the driver (unlike toPandas, which
    # stays as the baseline). Settings are read here on the driver because the
    # executor containers don't have the CLICKHOUSE_* variables.
    settings = {**connection_settings(), "autogenerate_session_id": False}
    if database:
        settings["database"] = database
    columns = df.columns

    def insert_partition(rows):
        client = clickhouse_connect.get_client(**settings)
        batch = []
        for row in rows:
            batch.append(tuple(row))
            if len(batch) >= batch_rows:
                client.insert(table, batch, column_names=columns)
                batch = []
        if batch:
            client.insert(table, batch, column_names=columns)
        client.close()

    df.foreachPartition(insert_partition)


def truncate_table(table):
    get_client().command(f"TRUNCATE TABLE IF EXISTS {table}")


def read_query(query):
    return get_client().query_df(query)
