"""
Unit tests for extraction history manager.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from phasor_point_cli.extraction_history import ExtractionHistory, ExtractionMetrics


class MockConfigPathManager:
    """Mock ConfigPathManager for testing."""

    def __init__(self, temp_dir: Path):
        self.temp_dir = temp_dir

    def get_local_config_file(self) -> Path:
        return self.temp_dir / "config.json"

    def get_user_config_dir(self) -> Path:
        return self.temp_dir


@pytest.fixture
def temp_history_dir(tmp_path):
    """Create temporary directory for history file."""
    return tmp_path


@pytest.fixture
def config_path_manager(temp_history_dir):
    """Create mock config path manager."""
    return MockConfigPathManager(temp_history_dir)


@pytest.fixture
def extraction_history(config_path_manager):
    """Create extraction history instance."""
    logger = MagicMock()
    return ExtractionHistory(config_path_manager, logger=logger)


class TestExtractionMetrics:
    """Test ExtractionMetrics dataclass."""

    def test_to_dict(self):
        """Test serialization to dict."""
        # Arrange
        metrics = ExtractionMetrics(
            timestamp="2025-10-27T10:30:45",
            rows=1000000,
            duration_sec=45.2,
            chunk_size_minutes=15,
            parallel_workers=3,
            rows_per_sec=22123.0,
        )

        # Act
        result = metrics.to_dict()

        # Assert
        assert result["timestamp"] == "2025-10-27T10:30:45"
        assert result["rows"] == 1000000
        assert result["duration_sec"] == 45.2
        assert result["chunk_size_minutes"] == 15
        assert result["parallel_workers"] == 3
        assert result["rows_per_sec"] == 22123.0

    def test_from_dict(self):
        """Test deserialization from dict."""
        # Arrange
        data = {
            "timestamp": "2025-10-27T10:30:45",
            "rows": 1000000,
            "duration_sec": 45.2,
            "chunk_size_minutes": 15,
            "parallel_workers": 3,
            "rows_per_sec": 22123.0,
        }

        # Act
        metrics = ExtractionMetrics.from_dict(data)

        # Assert
        assert metrics.timestamp == "2025-10-27T10:30:45"
        assert metrics.rows == 1000000
        assert metrics.duration_sec == 45.2
        assert metrics.chunk_size_minutes == 15
        assert metrics.parallel_workers == 3
        assert metrics.rows_per_sec == 22123.0


class TestExtractionHistory:
    """Test ExtractionHistory class."""

    def test_init(self, extraction_history):
        """Test initialization."""
        # Assert
        assert extraction_history is not None
        assert extraction_history._extractions == []

    def test_load_history_no_file(self, extraction_history):
        """Test loading when no history file exists."""
        # Act
        extraction_history.load_history()

        # Assert
        assert extraction_history._extractions == []

    def test_save_and_load_history(self, extraction_history):
        """Test saving and loading history."""
        # Arrange
        extraction_history.add_extraction(
            rows=1000000,
            duration_sec=45.2,
            chunk_size_minutes=15,
            parallel_workers=3,
        )

        # Flush to ensure pending changes are saved
        extraction_history.flush()

        # Act - create new instance and load
        new_history = ExtractionHistory(
            extraction_history.config_path_manager,
            logger=extraction_history.logger,
        )
        new_history.load_history()

        # Assert
        assert len(new_history._extractions) == 1
        assert new_history._extractions[0].rows == 1000000
        assert new_history._extractions[0].duration_sec == 45.2

    def test_add_extraction(self, extraction_history):
        """Test adding extraction record."""
        # Act
        extraction_history.add_extraction(
            rows=1000000,
            duration_sec=45.2,
            chunk_size_minutes=15,
            parallel_workers=3,
        )

        # Assert
        assert len(extraction_history._extractions) == 1
        assert extraction_history._extractions[0].rows == 1000000

    def test_add_extraction_calculates_rows_per_sec(self, extraction_history):
        """Test that rows per second is calculated correctly."""
        # Act
        extraction_history.add_extraction(
            rows=1000,
            duration_sec=10.0,
            chunk_size_minutes=15,
            parallel_workers=1,
        )

        # Assert
        assert extraction_history._extractions[0].rows_per_sec == 100.0

    def test_add_extraction_zero_duration(self, extraction_history):
        """Test that extraction with zero duration is rejected."""
        # Act
        extraction_history.add_extraction(
            rows=1000,
            duration_sec=0.0,
            chunk_size_minutes=15,
            parallel_workers=1,
        )

        # Assert
        assert len(extraction_history._extractions) == 0

    def test_get_average_rows_per_sec_no_history(self, extraction_history):
        """Test average calculation with no history."""
        # Act
        result = extraction_history.get_average_rows_per_sec()

        # Assert
        assert result is None

    def test_get_average_rows_per_sec(self, extraction_history):
        """Test average calculation with history."""
        # Arrange
        extraction_history.add_extraction(1000, 10.0, 15, 1)  # 100 rows/sec
        extraction_history.add_extraction(2000, 10.0, 15, 1)  # 200 rows/sec

        # Act
        result = extraction_history.get_average_rows_per_sec()

        # Assert
        assert result == 150.0

    def test_get_average_rows_per_sec_recent_n(self, extraction_history):
        """Test average calculation with recent_n parameter."""
        # Arrange
        extraction_history.add_extraction(1000, 10.0, 15, 1)  # 100 rows/sec
        extraction_history.add_extraction(2000, 10.0, 15, 1)  # 200 rows/sec
        extraction_history.add_extraction(3000, 10.0, 15, 1)  # 300 rows/sec

        # Act - only consider most recent 2
        result = extraction_history.get_average_rows_per_sec(recent_n=2)

        # Assert
        assert result == 250.0  # Average of 200 and 300

    def test_estimate_duration_no_history(self, extraction_history):
        """Test duration estimation with no history."""
        # Act
        result = extraction_history.estimate_duration(1000)

        # Assert
        assert result is None

    def test_estimate_duration(self, extraction_history):
        """Test duration estimation with history."""
        # Arrange
        extraction_history.add_extraction(1000, 10.0, 15, 1)  # 100 rows/sec

        # Act
        result = extraction_history.estimate_duration(500)

        # Assert
        assert result == 5.0  # 500 rows / 100 rows/sec = 5 seconds

    def test_history_pruning(self, extraction_history):
        """Test that old history entries are pruned."""
        # Arrange - add more than MAX_HISTORY_SIZE entries
        for i in range(ExtractionHistory.MAX_HISTORY_SIZE + 10):
            extraction_history.add_extraction(
                rows=1000 + i,
                duration_sec=10.0,
                chunk_size_minutes=15,
                parallel_workers=1,
            )

        # Act - reload to trigger pruning
        extraction_history.save_history()
        extraction_history.load_history()

        # Assert - should only keep MAX_HISTORY_SIZE entries
        assert len(extraction_history._extractions) == ExtractionHistory.MAX_HISTORY_SIZE

    def test_get_history_count(self, extraction_history):
        """Test getting history count."""
        # Arrange
        extraction_history.add_extraction(1000, 10.0, 15, 1)
        extraction_history.add_extraction(2000, 20.0, 15, 1)

        # Act
        count = extraction_history.get_history_count()

        # Assert
        assert count == 2
