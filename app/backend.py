"""Data-access layer for the SignalNow console.

The API and UI only ever call `DataSource`. Today it's backed by `MockDataSource`
(simulated stream); flip `USE_MOCK_BACKEND=false` and the identical interface is
served by `KafkaDataSource`, which tails the `freezer_alerts_enriched` (and metrics)
topic and aggregates in memory. Kafka is the only sink — there is no database in the
path. Nothing in the API or frontend changes.
"""
from __future__ import annotations

import os
from typing import Protocol


class DataSource(Protocol):
    def snapshot(self) -> dict:
        """Everything the UI needs for one refresh (see app.py for the contract)."""
        ...

    def set_burst(self, burst: bool) -> None:
        """Demo control. Real (Kafka) data reflects the live producer, so this
        is a no-op there; in mock mode it drives the simulated scenario."""
        ...

    def set_mode(self, mode: str) -> None:
        """Preview the UI under an 'rtm' or 'mb' latency profile (mock only)."""
        ...

    @property
    def is_mock(self) -> bool:
        ...


def get_data_source() -> DataSource:
    use_mock = os.getenv("USE_MOCK_BACKEND", "true").lower() == "true"
    if use_mock:
        from backend_mock import MockDataSource
        return MockDataSource()
    from backend_kafka import KafkaDataSource
    return KafkaDataSource()
