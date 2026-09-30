"""Shared ClickHouse connection and query helpers for Streamlit pages."""

from __future__ import annotations

import os

import clickhouse_connect
import pandas as pd
import streamlit as st

HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
PORT = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))
DB = os.environ.get("CLICKHOUSE_DB", "tourism")
USER = os.environ.get("CLICKHOUSE_USER", "default")
PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse")


@st.cache_resource
def client():
    return clickhouse_connect.get_client(
        host=HOST, port=PORT, username=USER, password=PASSWORD, database=DB,
        connect_timeout=10, send_receive_timeout=120,
    )


@st.cache_data(ttl=60, show_spinner="Querying ClickHouse...")
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    """
    Run a query and return a pandas DataFrame.

    Cache results for 60 seconds. Pass values through `params` using ClickHouse
    placeholders such as {sid:UInt32}; do not concatenate user values into SQL.
    """
    return client().query_df(sql, parameters=params or {})


def health() -> tuple[bool, str]:
    try:
        client().command("SELECT 1")
        return True, "ClickHouse is responding"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def table_exists(name: str) -> bool:
    try:
        n = client().command(
            "SELECT count() FROM system.tables "
            f"WHERE database = '{DB}' AND name = '{name}'")
        return int(n) > 0
    except Exception:  # noqa: BLE001
        return False


def require(tables: list[str], hint: str) -> bool:
    """
    Check connection, table existence, and row counts before rendering a page.
    """
    ok, msg = health()
    if not ok:
        st.error(
            f"Cannot connect to ClickHouse at `{HOST}:{PORT}`.\n\n```\n{msg[:400]}\n```\n\n"
            "Check in this order:\n"
            "1. `docker compose ps` — `dp-clickhouse` healthy?\n"
            "2. If you see `Connection refused`, ClickHouse may be listening only on localhost. "
            "See the runbook troubleshooting section.\n"
            "3. When running Streamlit outside Docker, set `CLICKHOUSE_HOST=localhost`."
        )
        return False

    missing = [t for t in tables if not table_exists(t)]
    if missing:
        st.error(f"Missing table: `{'`, `'.join(missing)}`\n\n{hint}")
        return False

    empty = []
    for t in tables:
        try:
            if int(client().command(f"SELECT count() FROM {DB}.{t}")) == 0:
                empty.append(t)
        except Exception:  # noqa: BLE001
            empty.append(t)

    if empty:
        st.warning(f"Table `{'`, `'.join(empty)}` is empty.\n\n{hint}")
        return False
    return True
