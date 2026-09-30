"""Shared ClickHouse query and Spark insertion helpers."""

from __future__ import annotations

import logging
from typing import Any, Iterable, Sequence

import clickhouse_connect
from pyspark.sql import DataFrame

from common.config import settings

log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════

def ch_client(**overrides: Any):
    """Create a ClickHouse client and close it after use."""
    kwargs = settings.ch_client_kwargs()
    kwargs.update(overrides)
    return clickhouse_connect.get_client(**kwargs)


def execute(sql: str) -> None:
    """Execute a command that returns no rows."""
    client = ch_client()
    try:
        client.command(sql)
    finally:
        client.close()


def scalar(sql: str) -> Any:
    """Return the first cell of a query, or None when empty."""
    client = ch_client()
    try:
        rows = client.query(sql).result_rows
        return rows[0][0] if rows else None
    finally:
        client.close()


def query_rows(sql: str) -> list[tuple]:
    client = ch_client()
    try:
        return client.query(sql).result_rows
    finally:
        client.close()


def truncate(table: str) -> None:
    """Clear a table before rebuilding it."""
    execute(f"TRUNCATE TABLE IF EXISTS {settings.ch_db}.{table}")
    log.info("TRUNCATE %s", table)


def drop_partitions(table: str, partitions: Iterable[str]) -> None:
    """Drop partitions."""
    for p in partitions:
        execute(f"ALTER TABLE {settings.ch_db}.{table} DROP PARTITION '{p}'")
        log.info("DROP PARTITION %s FROM %s", p, table)


def optimize(table: str, *, final: bool = True) -> None:
    """Optimize."""
    execute(f"OPTIMIZE TABLE {settings.ch_db}.{table}{' FINAL' if final else ''}")
    log.info("OPTIMIZE %s", table)


def reload_dictionary(name: str) -> None:
    """Reload a dictionary immediately."""
    execute(f"SYSTEM RELOAD DICTIONARY {settings.ch_db}.{name}")
    log.info("RELOAD DICTIONARY %s", name)


def table_count(table: str, *, final: bool = False) -> int:
    suffix = " FINAL" if final else ""
    return int(scalar(f"SELECT count() FROM {settings.ch_db}.{table}{suffix}") or 0)


def read_spark_df(spark, sql: str):
    """Read spark df."""
    client = ch_client()
    try:
        pdf = client.query_df(sql)
    finally:
        client.close()

    if pdf is None or len(pdf) == 0:
        raise ValueError(f"Query returned no rows:\n{sql}")

    return spark.createDataFrame(_normalise_dtypes(pdf))


def _normalise_dtypes(pdf):
    """ normalise dtypes."""
    import pandas as pd

    out = pdf.copy()
    for col in out.columns:
        dtype = out[col].dtype
        name = str(dtype)

        if getattr(dtype, "kind", None) == "u":              # uint8/16/32/64
            out[col] = out[col].astype("int64")
        elif name == "category":                             # LowCardinality
            out[col] = out[col].astype(str)
        elif name.startswith(("UInt", "Int")):
            if out[col].isna().any():
                out[col] = out[col].astype("float64")
            else:
                out[col] = out[col].astype("int64")
        elif name.startswith("Float"):
            out[col] = out[col].astype("float64")
        elif name == "boolean":
            out[col] = out[col].fillna(False).astype("bool")
    return out


# ══════════════════════════════════════════════════════════════════════
#  Ghi DataFrame → ClickHouse
# ══════════════════════════════════════════════════════════════════════

def write_df(
    df: DataFrame,
    table: str,
    *,
    columns: Sequence[str] | None = None,
    max_write_partitions: int = 8,
    batch_rows: int = 100_000,
) -> int:
    """Write df."""
    cols = list(columns) if columns else df.columns
    df_out = df.select(*cols)

    n_rows = df_out.count()
    if n_rows == 0:
        log.warning("No rows to write to %s; skipping.", table)
        return 0

    if df_out.rdd.getNumPartitions() > max_write_partitions:
        df_out = df_out.coalesce(max_write_partitions)



    conn = settings.ch_client_kwargs()
    db = settings.ch_db
    full_table = table
    col_names = cols
    chunk = batch_rows

    def _insert_partition(rows_iter) -> None:
        import clickhouse_connect as cc

        client = cc.get_client(**conn)
        try:
            buf: list[list] = []
            for row in rows_iter:
                buf.append(list(row))
                if len(buf) >= chunk:
                    client.insert(full_table, buf, column_names=col_names, database=db)
                    buf.clear()
            if buf:
                client.insert(full_table, buf, column_names=col_names, database=db)
        finally:
            client.close()

    df_out.rdd.foreachPartition(_insert_partition)
    log.info("Wrote %s rows to %s.%s", f"{n_rows:,}", db, table)
    return n_rows


def write_pandas(pdf, table: str, columns: Sequence[str] | None = None) -> int:
    """Write pandas."""
    if pdf is None or len(pdf) == 0:
        log.warning("Empty pandas DataFrame; nothing written to %s.", table)
        return 0

    cols = list(columns) if columns else list(pdf.columns)
    client = ch_client()
    try:
        client.insert(table, pdf[cols].values.tolist(),
                      column_names=cols, database=settings.ch_db)
    finally:
        client.close()
    log.info("Wrote %s rows to %s from the driver", f"{len(pdf):,}", table)
    return len(pdf)
