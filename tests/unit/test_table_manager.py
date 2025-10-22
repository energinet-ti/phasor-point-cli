"""
Unit tests for the TableManager class.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from phasor_point_cli.table_manager import TableManager


@pytest.fixture
def sample_config():
    """Create a mock config manager with get_pmu_info method."""
    from unittest.mock import Mock

    from phasor_point_cli.models import PMUInfo

    config = Mock()
    config.config = {
        "available_pmus": {
            "RegionA": [
                {"id": 45012, "station_name": "PMU A", "country": "NO"},
                {"id": 45013, "station_name": "PMU B", "country": "SE"},
            ]
        }
    }

    def get_pmu_info(pmu_id):
        for region, pmus in config.config["available_pmus"].items():
            for pmu_data in pmus:
                if pmu_data["id"] == pmu_id:
                    return PMUInfo(
                        id=pmu_data["id"],
                        station_name=pmu_data["station_name"],
                        region=region,
                        country=pmu_data.get("country", ""),
                    )
        return None

    config.get_pmu_info = get_pmu_info
    return config


def create_connection_mock(success_tables):
    def make_cursor():
        cursor = MagicMock()

        def execute_side_effect(query):
            for table in success_tables:
                if table in query:
                    return
            raise Exception("Table missing")

        cursor.execute.side_effect = execute_side_effect
        cursor.fetchone.return_value = (None,)
        cursor.nextset.return_value = None  # No more result sets
        cursor.close.return_value = None  # Allow cursor to be closed
        return cursor

    connection = MagicMock()
    # Return a new cursor instance each time cursor() is called
    connection.cursor.side_effect = make_cursor

    # For backwards compatibility, also store first cursor
    first_cursor = make_cursor()
    return connection, first_cursor


def test_list_available_tables_returns_expected_mapping(sample_config):
    # Arrange
    connection, _ = create_connection_mock({"pmu_45012_1"})
    pool = MagicMock()
    pool.get_connection.return_value = connection
    manager = TableManager(pool, sample_config, logger=MagicMock())

    # Act
    result = manager.list_available_tables(pmu_ids=[45012], resolutions=[1, 50])

    # Assert
    assert result.found_pmus == {45012: [1]}
    assert result.total_tables == 1
    pool.return_connection.assert_called_with(connection)
    # Verify cursor() was called once (reused for multiple queries in list_available_tables)
    assert connection.cursor.call_count == 1


@patch("phasor_point_cli.table_manager.pd.read_sql")
def test_get_table_info_returns_tableinfo(mock_read_sql, sample_config):
    # Arrange
    # Configure connections for access, stats, and sample data
    pool = MagicMock()

    access_conn = MagicMock()
    access_cursor = MagicMock()
    access_cursor.execute.return_value = None
    access_cursor.fetchone.return_value = (None,)
    access_cursor.nextset.return_value = None  # No more result sets
    access_cursor.close.return_value = None  # Allow cursor to be closed
    access_conn.cursor.return_value = access_cursor

    stats_conn = MagicMock()

    # Create multiple cursor instances for stats queries (one per query)
    def make_stats_cursor():
        cursor = MagicMock()

        def stats_execute_side_effect(query):
            if "COUNT" in query:
                cursor.fetchone.return_value = (100,)
            elif "MIN" in query:
                cursor.fetchone.return_value = (datetime(2025, 1, 1), datetime(2025, 1, 2))
            elif "TOP 0" in query:
                cursor.description = [("col1",), ("col2",), ("col3",)]

        cursor.execute.side_effect = stats_execute_side_effect
        cursor.nextset.return_value = None  # No more result sets
        cursor.close.return_value = None  # Allow cursor to be closed
        return cursor

    stats_conn.cursor.side_effect = make_stats_cursor

    sample_conn = MagicMock()
    mock_read_sql.return_value = pd.DataFrame({"value": [1, 2, 3]})

    pool.get_connection.side_effect = [access_conn, stats_conn, sample_conn]
    manager = TableManager(pool, sample_config, logger=MagicMock())

    # Act
    info = manager.get_table_info(45012, 1, sample_limit=3)

    # Assert
    assert info is not None
    assert info.table_name == "pmu_45012_1"
    # Row count is 0 because custom JDBC doesn't support COUNT
    assert info.statistics.row_count == 0
    assert info.statistics.column_count == 3
    assert info.sample_data is not None
    assert info.pmu_info.station_name == "PMU A"


def test_test_table_access_returns_false_on_error(sample_config):
    # Arrange
    pool = MagicMock()
    pool.get_connection.return_value = MagicMock()
    pool.get_connection.return_value.cursor.side_effect = Exception("boom")
    manager = TableManager(pool, sample_config, logger=MagicMock())

    # Act
    result = manager.test_table_access("pmu_45012_1")

    # Assert
    assert result is False
