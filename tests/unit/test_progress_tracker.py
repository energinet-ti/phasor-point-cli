"""
Unit tests for progress tracker.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from phasor_point_cli.extraction_history import ExtractionHistory
from phasor_point_cli.progress_tracker import ProgressTracker


@pytest.fixture
def mock_extraction_history():
    """Create mock extraction history."""
    history = MagicMock(spec=ExtractionHistory)
    history.get_history_count.return_value = 5
    history.get_average_rows_per_sec.return_value = 10000.0
    return history


@pytest.fixture
def progress_tracker(mock_extraction_history):
    """Create progress tracker instance."""
    logger = MagicMock()
    return ProgressTracker(
        extraction_history=mock_extraction_history,
        verbose_timing=False,
        logger=logger,
    )


@pytest.fixture
def progress_tracker_verbose(mock_extraction_history):
    """Create progress tracker with verbose timing."""
    logger = MagicMock()
    return ProgressTracker(
        extraction_history=mock_extraction_history,
        verbose_timing=True,
        logger=logger,
    )


class TestProgressTracker:
    """Test ProgressTracker class."""

    def test_init(self, progress_tracker):
        """Test initialization."""
        # Assert
        assert progress_tracker is not None
        assert progress_tracker.verbose_timing is False
        assert progress_tracker._total_chunks == 0

    def test_init_verbose(self, progress_tracker_verbose):
        """Test initialization with verbose timing."""
        # Assert
        assert progress_tracker_verbose.verbose_timing is True

    def test_start_extraction(self, progress_tracker):
        """Test starting extraction tracking."""
        # Act
        progress_tracker.start_extraction(total_chunks=10, pmu_id=123)

        # Assert
        assert progress_tracker._total_chunks == 10
        assert progress_tracker._current_pmu_id == 123
        assert progress_tracker._completed_chunks == 0

    @patch("sys.stdout")
    @patch("time.time")
    def test_update_chunk_progress_no_history(self, mock_time, mock_stdout, progress_tracker):
        """Test chunk progress update when no historical data."""
        # Arrange
        progress_tracker.extraction_history.get_history_count.return_value = 0
        mock_time.return_value = 100.0
        progress_tracker.start_extraction(total_chunks=10, pmu_id=123)
        progress_tracker._start_time = 90.0

        # Act - update first 2 chunks (should show "Calculating ETA...")
        progress_tracker.update_chunk_progress(0, 1000)
        progress_tracker.update_chunk_progress(1, 1000)

        # Assert - should show calculating ETA for first 2 chunks
        assert progress_tracker._completed_chunks == 2

    @patch("sys.stdout")
    @patch("time.time")
    def test_update_chunk_progress_with_history(self, mock_time, mock_stdout, progress_tracker):
        """Test chunk progress update with historical data."""
        # Arrange
        mock_time.return_value = 100.0
        progress_tracker.start_extraction(total_chunks=10, pmu_id=123, estimated_rows=10000)
        progress_tracker._start_time = 95.0

        # Act - with history, should show ETA after first chunk
        progress_tracker.update_chunk_progress(0, 1000)

        # Assert
        assert progress_tracker._completed_chunks == 1

    @patch("sys.stdout")
    @patch("time.time")
    def test_update_chunk_progress_calculates_eta(self, mock_time, mock_stdout, progress_tracker):
        """Test that ETA is calculated after enough chunks."""
        # Arrange
        progress_tracker.extraction_history.get_history_count.return_value = 0
        mock_time.return_value = 100.0
        progress_tracker.start_extraction(total_chunks=10, pmu_id=123)
        progress_tracker._start_time = 90.0

        # Act - update 3 chunks to trigger ETA calculation
        mock_time.return_value = 91.0
        progress_tracker.update_chunk_progress(0, 1000)
        mock_time.return_value = 92.0
        progress_tracker.update_chunk_progress(1, 1000)
        mock_time.return_value = 93.0
        progress_tracker.update_chunk_progress(2, 1000)

        # Assert - should have ETA now
        assert progress_tracker._completed_chunks == 3
        assert len(progress_tracker._chunk_times) == 3

    @patch("sys.stdout.isatty", return_value=True)
    @patch("builtins.print")
    def test_finish_extraction(self, mock_print, mock_isatty, progress_tracker):
        """Test finishing extraction."""
        # Arrange
        progress_tracker._is_tty = True  # Reflect mocked isatty
        progress_tracker.start_extraction(total_chunks=5, pmu_id=123)
        progress_tracker._completed_chunks = 5

        # Act
        progress_tracker.finish_extraction()

        # Assert - should have printed completion message
        assert mock_print.called

    def test_start_batch(self, progress_tracker):
        """Test starting batch tracking."""
        # Act
        progress_tracker.start_batch(total_pmus=15)

        # Assert
        assert progress_tracker._total_pmus == 15
        assert progress_tracker._completed_pmus == 0

    @patch("sys.stdout.isatty", return_value=True)
    @patch("builtins.print")
    @patch("time.time")
    def test_update_pmu_progress(self, mock_time, mock_print, mock_isatty, progress_tracker):
        """Test PMU progress update in batch."""
        # Arrange
        progress_tracker._is_tty = True  # Reflect mocked isatty
        mock_time.return_value = 100.0
        progress_tracker.start_batch(total_pmus=5)
        progress_tracker._batch_start_time = 80.0

        # Act
        progress_tracker.update_pmu_progress(0, 123)

        # Assert
        assert progress_tracker._completed_pmus == 1
        assert mock_print.called

    @patch("sys.stdout.isatty", return_value=True)
    @patch("builtins.print")
    def test_finish_batch(self, mock_print, mock_isatty, progress_tracker):
        """Test finishing batch."""
        # Arrange
        progress_tracker._is_tty = True  # Reflect mocked isatty
        progress_tracker.start_batch(total_pmus=5)
        progress_tracker._completed_pmus = 5

        # Act
        progress_tracker.finish_batch()

        # Assert - should have printed completion message
        assert mock_print.called

    def test_format_time_seconds(self, progress_tracker):
        """Test time formatting for seconds."""
        # Act
        result = progress_tracker._format_time(45.0)

        # Assert
        assert result == "45s"

    def test_format_time_minutes(self, progress_tracker):
        """Test time formatting for minutes."""
        # Act
        result = progress_tracker._format_time(125.0)

        # Assert
        assert result == "2m 5s"

    def test_format_time_hours(self, progress_tracker):
        """Test time formatting for hours."""
        # Act
        result = progress_tracker._format_time(5430.0)

        # Assert
        assert result == "1h 30m"

    def test_format_time_negative(self, progress_tracker):
        """Test time formatting handles negative values."""
        # Act
        result = progress_tracker._format_time(-10.0)

        # Assert
        assert result == "0s"

    def test_progress_tracker_with_output(self, mock_extraction_history):
        """Test progress tracker uses UserOutput when provided."""
        # Arrange
        mock_output = Mock()
        logger = MagicMock()

        with patch("sys.stdout.isatty", return_value=True):
            tracker = ProgressTracker(
                extraction_history=mock_extraction_history,
                verbose_timing=False,
                logger=logger,
                output=mock_output,
            )

            # Act
            tracker.start_batch(total_pmus=3)
            tracker.update_pmu_progress(pmu_index=0, pmu_id=45012)

        # Assert
        mock_output.batch_progress.assert_called_once_with(1, 3, 45012)
