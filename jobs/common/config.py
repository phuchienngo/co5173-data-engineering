"""Shared settings and business thresholds."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Settings:
    # ── ClickHouse ──────────────────────────────────────────────────
    ch_host: str = field(default_factory=lambda: _env("CLICKHOUSE_HOST", "clickhouse"))
    ch_http_port: int = field(default_factory=lambda: _env_int("CLICKHOUSE_HTTP_PORT", 8123))
    ch_db: str = field(default_factory=lambda: _env("CLICKHOUSE_DB", "tourism"))
    ch_user: str = field(default_factory=lambda: _env("CLICKHOUSE_USER", "default"))
    ch_password: str = field(default_factory=lambda: _env("CLICKHOUSE_PASSWORD", "clickhouse"))

    # ── Kafka ───────────────────────────────────────────────────────
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "kafka:29092"))
    kafka_topic_flights: str = field(default_factory=lambda: _env("KAFKA_TOPIC_FLIGHTS", "flight_events"))


    data_root: str = field(default_factory=lambda: _env("DATA_ROOT", "/opt/data"))

    # ── Spark ───────────────────────────────────────────────────────
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER_URL", "spark://spark-master:7077"))

    # ────────────────────────────────────────────────────────────────

    # ────────────────────────────────────────────────────────────────
    window_minutes: int = 15








    #




    flow_watch: int = 100
    flow_warning: int = 120
    flow_critical: int = 150
    flow_emergency: int = 200




    baseline_floor: int = 5





    delay_min_arrivals: int = 4
    delay_share_watch: float = 0.6

    # ────────────────────────────────────────────────────────────────

    # ────────────────────────────────────────────────────────────────

    occ_watch: int = 70
    occ_warning: int = 85
    occ_critical: int = 95

    occ_horizon_nights: int = 90

    # ────────────────────────────────────────────────────────────────

    # ────────────────────────────────────────────────────────────────


    cohort_start: str = "2022-01-01"

    cohort_drop_last_months: int = 2
    min_cohort_size: int = 50

    same_trip_days: int = 3


    @property
    def raw_dir(self) -> str:
        return f"{self.data_root}/raw"

    @property
    def lake_dir(self) -> str:
        return f"{self.data_root}/lake"

    @property
    def checkpoint_dir(self) -> str:
        return f"{self.data_root}/checkpoints"

    @property
    def report_dir(self) -> str:
        return f"{self.data_root}/reports"

    def ch_client_kwargs(self) -> dict:
        """Ch client kwargs."""
        return dict(
            host=self.ch_host,
            port=self.ch_http_port,
            username=self.ch_user,
            password=self.ch_password,
            database=self.ch_db,
        )


settings = Settings()


ALERT_LEVELS = {
    0: "NORMAL",
    1: "WATCH",
    2: "WARNING",
    3: "CRITICAL",
    4: "EMERGENCY",
}
