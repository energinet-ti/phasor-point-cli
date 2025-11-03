"""
Unit tests for PMU metadata retrieval and merging.
"""

from unittest.mock import MagicMock, Mock

import pytest

from phasor_point_cli.pmu_metadata import fetch_pmu_metadata_from_database, merge_pmu_metadata


@pytest.fixture
def connection_pool():
    """Mock connection pool fixture following same pattern as test_data_extractor.py"""
    pool = MagicMock()
    pool.get_connection.return_value = MagicMock()
    return pool


class TestFetchPmuMetadataFromDatabase:
    """Tests for fetch_pmu_metadata_from_database function."""

    def test_fetch_success(self, connection_pool):
        """Test successful PMU metadata fetch from database."""
        # Arrange
        mock_row1 = Mock()
        mock_row1.id = 501
        mock_row1.station_name = "Bassecourt"

        mock_row2 = Mock()
        mock_row2.id = 901
        mock_row2.station_name = "KEMINMAA"

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [mock_row1, mock_row2]

        mock_conn = connection_pool.get_connection.return_value
        mock_conn.cursor.return_value = mock_cursor

        # Act
        result = fetch_pmu_metadata_from_database(connection_pool)

        # Assert
        assert len(result) == 2
        assert result[0] == {"id": 501, "station_name": "Bassecourt"}
        assert result[1] == {"id": 901, "station_name": "KEMINMAA"}
        connection_pool.get_connection.assert_called_once()
        connection_pool.return_connection.assert_called_once_with(mock_conn)
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_fetch_with_null_station_name(self, connection_pool):
        """Test PMU metadata fetch with NULL station name uses default."""
        # Arrange
        mock_row = Mock()
        mock_row.id = 999
        mock_row.station_name = None

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [mock_row]

        mock_conn = connection_pool.get_connection.return_value
        mock_conn.cursor.return_value = mock_cursor

        # Act
        result = fetch_pmu_metadata_from_database(connection_pool)

        # Assert
        assert len(result) == 1
        assert result[0] == {"id": 999, "station_name": "PMU 999"}

    def test_fetch_with_null_id_skipped(self, connection_pool):
        """Test PMU metadata fetch skips rows with NULL id."""
        # Arrange
        mock_row1 = Mock()
        mock_row1.id = None
        mock_row1.station_name = "Invalid"

        mock_row2 = Mock()
        mock_row2.id = 501
        mock_row2.station_name = "Valid"

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [mock_row1, mock_row2]

        mock_conn = connection_pool.get_connection.return_value
        mock_conn.cursor.return_value = mock_cursor

        # Act
        result = fetch_pmu_metadata_from_database(connection_pool)

        # Assert
        assert len(result) == 1
        assert result[0] == {"id": 501, "station_name": "Valid"}

    def test_fetch_no_connection_available(self, connection_pool):
        """Test fetch raises error when connection pool returns None."""
        # Arrange
        connection_pool.get_connection.return_value = None

        # Act & Assert
        with pytest.raises(RuntimeError, match="Failed to obtain database connection"):
            fetch_pmu_metadata_from_database(connection_pool)

    def test_fetch_with_logger(self, connection_pool):
        """Test fetch logs appropriate messages."""
        # Arrange
        mock_logger = Mock()

        mock_row = Mock()
        mock_row.id = 501
        mock_row.station_name = "Test"

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [mock_row]

        mock_conn = connection_pool.get_connection.return_value
        mock_conn.cursor.return_value = mock_cursor

        # Act
        fetch_pmu_metadata_from_database(connection_pool, logger=mock_logger)

        # Assert
        assert mock_logger.info.call_count >= 2  # At least "Fetching" and "Successfully fetched"


class TestMergePmuMetadata:
    """Tests for merge_pmu_metadata function."""

    def test_merge_empty_existing_with_new(self):
        """Test merging when existing list is empty."""
        # Arrange
        existing = []
        new_pmus = [
            {"id": 501, "station_name": "Bassecourt"},
            {"id": 901, "station_name": "KEMINMAA"},
        ]

        # Act
        result = merge_pmu_metadata(existing, new_pmus)

        # Assert
        assert len(result) == 2
        assert result[0] == {"id": 501, "station_name": "Bassecourt"}
        assert result[1] == {"id": 901, "station_name": "KEMINMAA"}

    def test_merge_existing_with_empty_new(self):
        """Test merging when new list is empty (keeps existing)."""
        # Arrange
        existing = [
            {"id": 501, "station_name": "Bassecourt"},
            {"id": 901, "station_name": "KEMINMAA"},
        ]
        new_pmus = []

        # Act
        result = merge_pmu_metadata(existing, new_pmus)

        # Assert
        assert len(result) == 2
        assert result[0] == {"id": 501, "station_name": "Bassecourt"}
        assert result[1] == {"id": 901, "station_name": "KEMINMAA"}

    def test_merge_updates_existing_name(self):
        """Test merge updates existing PMU station_name."""
        # Arrange
        existing = [{"id": 501, "station_name": "Old Name", "custom": "preserved"}]
        new_pmus = [{"id": 501, "station_name": "New Name"}]

        # Act
        result = merge_pmu_metadata(existing, new_pmus)

        # Assert
        assert len(result) == 1
        assert result[0]["station_name"] == "New Name"
        assert result[0]["custom"] == "preserved"

    def test_merge_adds_new_pmus(self):
        """Test merge adds new PMUs not in existing."""
        # Arrange
        existing = [{"id": 501, "station_name": "Bassecourt"}]
        new_pmus = [
            {"id": 501, "station_name": "Bassecourt"},
            {"id": 901, "station_name": "KEMINMAA"},
        ]

        # Act
        result = merge_pmu_metadata(existing, new_pmus)

        # Assert
        assert len(result) == 2
        assert any(p["id"] == 901 for p in result)

    def test_merge_preserves_custom_fields(self):
        """Test merge preserves custom fields in existing PMUs."""
        # Arrange
        existing = [{"id": 501, "station_name": "Old", "custom": "data"}]
        new_pmus = [{"id": 501, "station_name": "New"}]

        # Act
        result = merge_pmu_metadata(existing, new_pmus)

        # Assert
        assert result[0]["station_name"] == "New"
        assert result[0]["custom"] == "data"

    def test_merge_adds_new_fields_from_new_pmus(self):
        """Test merge adds new fields from new PMU data."""
        # Arrange
        existing = [{"id": 501, "station_name": "Old"}]
        new_pmus = [{"id": 501, "station_name": "New", "voltage": "400kV"}]

        # Act
        result = merge_pmu_metadata(existing, new_pmus)

        # Assert
        assert result[0]["station_name"] == "New"
        assert result[0]["voltage"] == "400kV"

    def test_merge_sorts_by_id(self):
        """Test merge returns sorted list by PMU ID."""
        # Arrange
        existing = [{"id": 901, "station_name": "B"}]
        new_pmus = [{"id": 501, "station_name": "A"}, {"id": 1000, "station_name": "C"}]

        # Act
        result = merge_pmu_metadata(existing, new_pmus)

        # Assert
        assert len(result) == 3
        assert result[0]["id"] == 501
        assert result[1]["id"] == 901
        assert result[2]["id"] == 1000

    def test_merge_skips_pmus_without_id(self):
        """Test merge skips PMUs without an id field."""
        # Arrange
        existing = [{"station_name": "No ID"}]
        new_pmus = [{"id": 501, "station_name": "Valid"}, {"station_name": "Also No ID"}]

        # Act
        result = merge_pmu_metadata(existing, new_pmus)

        # Assert
        assert len(result) == 1
        assert result[0]["id"] == 501

    def test_merge_handles_none_id(self):
        """Test merge skips PMUs with None as id."""
        # Arrange
        existing = [{"id": None, "station_name": "Invalid"}]
        new_pmus = [
            {"id": 501, "station_name": "Valid"},
            {"id": None, "station_name": "Also Invalid"},
        ]

        # Act
        result = merge_pmu_metadata(existing, new_pmus)

        # Assert
        assert len(result) == 1
        assert result[0]["id"] == 501

    def test_merge_complex_scenario(self):
        """Test merge with multiple updates, additions, and preserved PMUs."""
        # Arrange
        existing = [
            {"id": 501, "station_name": "Old Name", "custom": "preserved"},
            {"id": 901, "station_name": "KEMINMAA"},
            {"id": 1000, "station_name": "Keep Me"},
        ]
        new_pmus = [
            {"id": 501, "station_name": "New Name"},  # Update
            {"id": 1026, "station_name": "New PMU"},  # Add
            {"id": 2000, "station_name": "Another New"},  # Add
        ]

        # Act
        result = merge_pmu_metadata(existing, new_pmus)

        # Assert
        assert len(result) == 5
        # Check update
        pmu_501 = next(p for p in result if p["id"] == 501)
        assert pmu_501["station_name"] == "New Name"
        assert pmu_501["custom"] == "preserved"
        # Check preserved
        assert any(p["id"] == 901 and p["station_name"] == "KEMINMAA" for p in result)
        assert any(p["id"] == 1000 and p["station_name"] == "Keep Me" for p in result)
        # Check additions
        assert any(p["id"] == 1026 and p["station_name"] == "New PMU" for p in result)
        assert any(p["id"] == 2000 and p["station_name"] == "Another New" for p in result)
        # Check sorting
        assert result[0]["id"] < result[1]["id"] < result[2]["id"]
