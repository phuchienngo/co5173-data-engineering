"""Bounded partition writes; deterministic visit keys make retries idempotent."""

from datetime import date, datetime


def write_partition(rows, config):
    import clickhouse_connect

    client = clickhouse_connect.get_client(**config)
    columns = ["event_id", "user_id", "place_id", "visit_date", "visit_time", "raw_checkins"]
    pending = []
    try:
        for row in rows:
            record = [row[column] for column in columns]
            if isinstance(record[3], str):
                record[3] = date.fromisoformat(record[3])
            if isinstance(record[4], str):
                record[4] = datetime.fromisoformat(record[4].replace("Z", "+00:00"))
            pending.append(record)
            if len(pending) >= 1000:
                client.insert("fact_br8_visits", pending, column_names=columns)
                pending = []
        if pending:
            client.insert("fact_br8_visits", pending, column_names=columns)
    finally:
        client.close()
