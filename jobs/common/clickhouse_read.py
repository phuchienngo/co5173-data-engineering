"""Reading a ClickHouse query into Spark, in parallel or through the driver.

Built from BR5's reader (jobs/br5_promotion/pipeline_spark.py), which keeps its
own copy. The query, the slice column and the schema are parameters. Values
must be read as signed types: Spark's Arrow conversion has no unsigned integers.
"""

import clickhouse_connect

from clickhouse_io import connection_settings


def read_parallel(spark, select, where, slice_column, partitions, schema, query_settings=None):
    """Each Spark task reads its own slice (slice_column % N = i) as Arrow
    batches, with its own ClickHouse client. Connection settings are taken here
    on the driver: the worker container has no CLICKHOUSE_* variables."""
    conn = {**connection_settings(), "autogenerate_session_id": False}
    settings = dict(query_settings or {})
    names = schema.fieldNames()
    base = f"{select} WHERE ({where}) AND {slice_column} % {int(partitions)} = "

    def read_slices(batches):
        client = clickhouse_connect.get_client(**conn)
        try:
            for batch in batches:
                for slice_no in batch.column(0).to_pylist():
                    table = client.query_arrow(base + str(slice_no), settings=settings)
                    yield from table.select(names).to_batches()
        finally:
            client.close()

    return spark.range(0, partitions, 1, partitions).mapInArrow(read_slices, schema)


def read_driver(spark, client, select, where, schema, query_settings=None):
    """Baseline: one query on the driver; the rows are then shipped to the
    executors from there."""
    table = client.query_arrow(f"{select} WHERE {where}", settings=dict(query_settings or {}))
    return spark.createDataFrame(table.select(schema.fieldNames()))
