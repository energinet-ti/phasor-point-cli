"""
Unit tests for the ExtractionManager class.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from phasor_point_cli.extraction_history import ExtractionHistory
from phasor_point_cli.extraction_manager import ExtractionManager
from phasor_point_cli.models import DateRange, ExtractionRequest, ExtractionResult


class ConfigStub:
    def __init__(self, data):
        self.config = data

    def get_pmu_info(self, pmu_id):
        """Get PMU info from config."""
        from phasor_point_cli.models import PMUInfo

        for pmu_data in self.config.get("available_pmus", []):
            if pmu_data["id"] == pmu_id:
                return PMUInfo(
                    id=pmu_data["id"],
                    station_name=pmu_data["station_name"],
                    country=pmu_data.get("country", ""),
                )
        return None


class MockConfigPathManager:
    """Mock ConfigPathManager for testing."""

    def __init__(self, temp_dir: Path):
        self.temp_dir = temp_dir

    def get_local_config_file(self) -> Path:
        return self.temp_dir / "config.json"

    def get_user_config_dir(self) -> Path:
        return self.temp_dir


@pytest.fixture
def mock_extraction_history(tmp_path):
    """Create mock extraction history that uses temp directory."""
    logger = MagicMock()
    config_path_manager = MockConfigPathManager(tmp_path)
    return ExtractionHistory(config_path_manager, logger=logger)


def build_request(tmp_path: Path) -> ExtractionRequest:
    date_range = DateRange(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 0, 10, 0),
    )
    return ExtractionRequest(
        pmu_id=45012,
        date_range=date_range,
        output_file=tmp_path / "output.csv",
        resolution=1,
        processed=True,
        clean=True,
        chunk_size_minutes=15,
        parallel_workers=1,
        output_format="csv",
    )


def test_extraction_manager_success(tmp_path, mock_extraction_history):
    # Arrange
    df_raw = pd.DataFrame(
        {
            "ts": pd.date_range(datetime(2025, 1, 1, 0, 0, 0), periods=4, freq="min"),
            "value": [1, 2, 3, 4],
        }
    )
    df_processed = df_raw.copy()

    extractor = MagicMock()
    extractor.extract.return_value = df_raw

    processor = MagicMock()
    processor.process.return_value = (df_processed, [])

    power_calculator = MagicMock()
    power_calculator.process_phasor_data.return_value = (df_processed, None)

    logger = MagicMock()
    config = {"available_pmus": [{"number": 45012, "name": "PMU A", "country": "NO"}]}

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub(config),
        logger=logger,
        data_extractor=extractor,
        data_processor=processor,
        power_calculator=power_calculator,
        extraction_history=mock_extraction_history,
    )
    request = build_request(tmp_path)

    # Act
    result = manager.extract(request)

    # Assert
    assert result.success is True
    assert result.output_file is not None
    assert result.rows_extracted == len(df_processed)
    assert Path(result.output_file).exists()
    assert Path(str(result.output_file).replace(".csv", "_extraction_log.json")).exists()
    extractor.extract.assert_called_once()
    assert extractor.extract.call_args[0][0] == request  # Check request is passed
    processor.process.assert_called()
    power_calculator.process_phasor_data.assert_called()


def test_extraction_manager_handles_empty_extraction(tmp_path, mock_extraction_history):
    # Arrange
    extractor = MagicMock()
    extractor.extract.return_value = None

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        data_extractor=extractor,
        data_processor=MagicMock(),
        power_calculator=MagicMock(),
        extraction_history=mock_extraction_history,
    )
    request = build_request(tmp_path)

    # Act
    result = manager.extract(request)

    # Assert
    assert result.success is False
    assert result.output_file is None


def test_batch_extract_success(tmp_path, mock_extraction_history):
    """Test successful batch extraction of multiple PMUs."""
    # Arrange
    df_raw = pd.DataFrame(
        {
            "ts": pd.date_range(datetime(2025, 1, 1, 0, 0, 0), periods=4, freq="min"),
            "value": [1, 2, 3, 4],
        }
    )
    df_processed = df_raw.copy()

    extractor = MagicMock()
    extractor.extract.return_value = df_raw

    processor = MagicMock()
    processor.process.return_value = (df_processed, [])

    power_calculator = MagicMock()
    power_calculator.process_phasor_data.return_value = (df_processed, None)

    logger = MagicMock()
    config = {
        "available_pmus": [
            {"id": 45012, "station_name": "PMU A", "country": "NO"},
            {"id": 45013, "station_name": "PMU B", "country": "NO"},
        ]
    }

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub(config),
        logger=logger,
        data_extractor=extractor,
        data_processor=processor,
        power_calculator=power_calculator,
        extraction_history=mock_extraction_history,
    )

    date_range = DateRange(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 0, 10, 0),
    )

    requests = [
        ExtractionRequest(
            pmu_id=45012,
            date_range=date_range,
            resolution=1,
            processed=True,
            clean=True,
            output_format="csv",
        ),
        ExtractionRequest(
            pmu_id=45013,
            date_range=date_range,
            resolution=1,
            processed=True,
            clean=True,
            output_format="csv",
        ),
    ]

    # Act
    batch_result = manager.batch_extract(requests, output_dir=tmp_path)

    # Assert
    assert batch_result.batch_id is not None
    assert len(batch_result.results) == 2
    assert len(batch_result.successful_results()) == 2
    assert len(batch_result.failed_results()) == 0
    assert all(result.success for result in batch_result.results)


def test_batch_extract_partial_failure(tmp_path, mock_extraction_history):
    """Test batch extraction with some failures."""
    # Arrange
    df_raw = pd.DataFrame(
        {
            "ts": pd.date_range(datetime(2025, 1, 1, 0, 0, 0), periods=4, freq="min"),
            "value": [1, 2, 3, 4],
        }
    )
    df_processed = df_raw.copy()

    call_count = 0

    def mock_extract(request, chunk_strategy=None, progress_tracker=None):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return df_raw
        raise ValueError("Simulated extraction failure")

    extractor = MagicMock()
    extractor.extract.side_effect = mock_extract

    processor = MagicMock()
    processor.process.return_value = (df_processed, [])

    power_calculator = MagicMock()
    power_calculator.process_phasor_data.return_value = (df_processed, None)

    logger = MagicMock()
    config = {
        "available_pmus": [
            {"id": 45012, "station_name": "PMU A", "country": "NO"},
            {"id": 45013, "station_name": "PMU B", "country": "NO"},
        ]
    }

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub(config),
        logger=logger,
        data_extractor=extractor,
        data_processor=processor,
        power_calculator=power_calculator,
        extraction_history=mock_extraction_history,
    )

    date_range = DateRange(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 0, 10, 0),
    )

    requests = [
        ExtractionRequest(
            pmu_id=45012,
            date_range=date_range,
            resolution=1,
            processed=True,
            clean=True,
            output_format="csv",
        ),
        ExtractionRequest(
            pmu_id=45013,
            date_range=date_range,
            resolution=1,
            processed=True,
            clean=True,
            output_format="csv",
        ),
    ]

    # Act
    batch_result = manager.batch_extract(requests, output_dir=tmp_path)

    # Assert
    assert len(batch_result.results) == 2
    assert len(batch_result.successful_results()) == 1
    assert len(batch_result.failed_results()) == 1
    assert batch_result.failed_results()[0].error == "Simulated extraction failure"


# ============================================================================
# CRITICAL ERROR HANDLING TESTS - Priority 4
# ============================================================================


def test_extraction_request_validates_output_format():
    """Test that ExtractionRequest validation catches unsupported output format."""
    # Arrange
    date_range = DateRange(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 0, 10, 0),
    )

    # Act & Assert - Creating request with invalid format should raise ValueError
    with pytest.raises(ValueError) as exc_info:
        request = ExtractionRequest(
            pmu_id=45012,
            date_range=date_range,
            resolution=1,
            processed=True,
            clean=True,
            output_format="xlsx",  # Unsupported format
        )
        request.validate()  # Trigger validation

    assert "output_format" in str(exc_info.value).lower()


def test_extraction_log_write_failure_continues_gracefully(
    tmp_path, mock_extraction_history, monkeypatch
):
    """Test that extraction log write failures don't crash the extraction."""
    # Arrange
    df = pd.DataFrame({"ts": [1, 2, 3], "value": [1, 2, 3]})

    extractor = MagicMock()
    extractor.extract.return_value = df

    processor = MagicMock()
    processor.process.return_value = (df, [])

    power_calculator = MagicMock()
    power_calculator.process_phasor_data.return_value = (df, None)

    logger = MagicMock()
    config = {"available_pmus": [{"id": 45012, "station_name": "PMU A", "country": "NO"}]}

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub(config),
        logger=logger,
        data_extractor=extractor,
        data_processor=processor,
        power_calculator=power_calculator,
        extraction_history=mock_extraction_history,
    )

    request = build_request(tmp_path)

    # Mock json.dump to raise error

    original_dump = json.dump

    def failing_dump(*args, **kwargs):
        if "extraction_log" in str(args):
            raise PermissionError("Cannot write log file")
        return original_dump(*args, **kwargs)

    monkeypatch.setattr("json.dump", failing_dump)

    # Act - Should complete extraction even if log write fails
    result = manager.extract(request)

    # Assert - Extraction should succeed even if log write fails
    assert result.success is True
    assert result.output_file is not None


def test_extraction_log_read_failure_handled(tmp_path, mock_extraction_history):
    """Test that corrupted extraction log is handled gracefully."""
    # Arrange
    df = pd.DataFrame({"ts": [1, 2, 3], "value": [1, 2, 3]})

    extractor = MagicMock()
    extractor.extract.return_value = df

    processor = MagicMock()
    processor.process.return_value = (df, [])

    power_calculator = MagicMock()
    power_calculator.process_phasor_data.return_value = (df, None)

    logger = MagicMock()
    config = {"available_pmus": [{"id": 45012, "station_name": "PMU A", "country": "NO"}]}

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub(config),
        logger=logger,
        data_extractor=extractor,
        data_processor=processor,
        power_calculator=power_calculator,
        extraction_history=mock_extraction_history,
    )

    output_file = tmp_path / "output.csv"
    log_file = tmp_path / "output_extraction_log.json"

    # Create existing output file
    output_file.write_text("old,data\n1,2\n")

    # Create corrupted log file
    log_file.write_text("not valid json{}", encoding="utf-8")

    date_range = DateRange(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 0, 10, 0),
    )

    request = ExtractionRequest(
        pmu_id=45012,
        date_range=date_range,
        output_file=output_file,
        resolution=1,
        processed=True,
        clean=True,
        output_format="csv",
        replace=False,  # Default behavior
    )

    # Act - Should handle corrupted log gracefully
    result = manager.extract(request)

    # Assert - Should proceed with extraction despite corrupted log
    assert result.success is True
    logger.warning.assert_called()  # Should log warning about corrupted log


def test_batch_extract_all_failures_returns_summary(tmp_path, mock_extraction_history):
    """Test that batch extraction with all failures returns comprehensive summary."""
    # Arrange
    extractor = MagicMock()
    extractor.extract.side_effect = Exception("Database connection lost")

    logger = MagicMock()
    config = {
        "available_pmus": [
            {"id": 45012, "station_name": "PMU A", "country": "NO"},
            {"id": 45013, "station_name": "PMU B", "country": "NO"},
        ]
    }

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub(config),
        logger=logger,
        data_extractor=extractor,
        data_processor=MagicMock(),
        power_calculator=MagicMock(),
        extraction_history=mock_extraction_history,
    )

    date_range = DateRange(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 0, 10, 0),
    )

    requests = [
        ExtractionRequest(
            pmu_id=45012,
            date_range=date_range,
            resolution=1,
            processed=True,
            clean=True,
            output_format="csv",
        ),
        ExtractionRequest(
            pmu_id=45013,
            date_range=date_range,
            resolution=1,
            processed=True,
            clean=True,
            output_format="csv",
        ),
    ]

    # Act
    batch_result = manager.batch_extract(requests, output_dir=tmp_path)

    # Assert
    assert len(batch_result.results) == 2
    assert len(batch_result.successful_results()) == 0
    assert len(batch_result.failed_results()) == 2
    assert all(
        r.error is not None and "Database connection lost" in r.error
        for r in batch_result.failed_results()
    )


def test_get_local_timezone_invalid_tz_warns(monkeypatch):
    """Test that invalid TZ environment variable issues warning and falls back."""
    from phasor_point_cli.date_utils import DateRangeCalculator

    # Arrange
    monkeypatch.setenv("TZ", "Invalid/Timezone")

    # Act & Assert
    with pytest.warns(
        UserWarning,
        match="Invalid timezone in TZ environment variable: 'Invalid/Timezone'",
    ):
        result = DateRangeCalculator.get_local_timezone()

    # Should still return a timezone (system fallback)
    assert result is not None


def test_get_utc_offset_summer_date_copenhagen(monkeypatch):
    """Test UTC offset calculation for summer date (DST active)."""
    from phasor_point_cli.date_utils import DateRangeCalculator

    # Arrange
    monkeypatch.setenv("TZ", "Europe/Copenhagen")
    dt = datetime(2024, 7, 15, 10, 0, 0)

    # Act
    result = DateRangeCalculator.get_utc_offset(dt)

    # Assert
    # Copenhagen summer time is UTC+2 (CEST)
    assert result == "+02:00"


def test_get_utc_offset_winter_date_copenhagen(monkeypatch):
    """Test UTC offset calculation for winter date (DST inactive)."""
    from phasor_point_cli.date_utils import DateRangeCalculator

    # Arrange
    monkeypatch.setenv("TZ", "Europe/Copenhagen")
    dt = datetime(2024, 1, 15, 10, 0, 0)

    # Act
    result = DateRangeCalculator.get_utc_offset(dt)

    # Assert
    # Copenhagen winter time is UTC+1 (CET)
    assert result == "+01:00"


def test_get_utc_offset_different_dates_different_offsets(monkeypatch):
    """Test that same local time gets different UTC offsets based on date."""
    from phasor_point_cli.date_utils import DateRangeCalculator

    # Arrange
    monkeypatch.setenv("TZ", "Europe/Copenhagen")

    summer_dt = datetime(2024, 7, 15, 14, 0, 0)
    winter_dt = datetime(2024, 1, 15, 14, 0, 0)

    # Act
    summer_offset = DateRangeCalculator.get_utc_offset(summer_dt)
    winter_offset = DateRangeCalculator.get_utc_offset(winter_dt)

    # Assert
    # Same local time (14:00), different offsets due to DST
    assert summer_offset == "+02:00"
    assert winter_offset == "+01:00"


def test_get_local_timezone_returns_dst_aware_timezone(monkeypatch):
    """Test that _get_local_timezone returns a DST-aware timezone object."""
    from phasor_point_cli.date_utils import DateRangeCalculator

    # Arrange
    monkeypatch.setenv("TZ", "Europe/Copenhagen")

    # Act
    tz = DateRangeCalculator.get_local_timezone()

    # Assert
    assert tz is not None
    # Verify it can calculate different offsets for different dates
    summer_dt = datetime(2024, 7, 15, 10, 0, 0)
    winter_dt = datetime(2024, 1, 15, 10, 0, 0)

    summer_offset = DateRangeCalculator.get_utc_offset(summer_dt)
    winter_offset = DateRangeCalculator.get_utc_offset(winter_dt)

    # If it's DST-aware, offsets should differ
    assert summer_offset != winter_offset


# ============================================================================
# HELPER METHOD UNIT TESTS
# ============================================================================


def test_build_failure_result_default_rows(tmp_path, mock_extraction_history):
    """Test _build_failure_result with default rows=0."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )
    request = build_request(tmp_path)

    # Act
    result = manager._build_failure_result(request, 5.5, "Test error")

    # Assert
    assert result.success is False
    assert result.output_file is None
    assert result.rows_extracted == 0
    assert result.extraction_time_seconds == 5.5
    assert result.error == "Test error"
    assert result.request == request


def test_build_failure_result_custom_rows(tmp_path, mock_extraction_history):
    """Test _build_failure_result with custom row count."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )
    request = build_request(tmp_path)

    # Act
    result = manager._build_failure_result(request, 10.2, "Another error", rows=500)

    # Assert
    assert result.success is False
    assert result.rows_extracted == 500
    assert result.error == "Another error"


def test_handle_skip_existing_file_does_not_exist(tmp_path, mock_extraction_history):
    """Test _handle_skip_existing_file when file doesn't exist."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )
    request = build_request(tmp_path)
    output_path = tmp_path / "nonexistent.csv"

    # Act
    result = manager._handle_skip_existing_file(request, output_path, 0.0)

    # Assert
    assert result is None


def test_handle_skip_existing_file_replace_enabled(tmp_path, mock_extraction_history):
    """Test _handle_skip_existing_file when replace=True."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )
    request = build_request(tmp_path)
    request.replace = True
    output_path = tmp_path / "output.csv"
    output_path.write_text("existing data")

    # Act
    result = manager._handle_skip_existing_file(request, output_path, 0.0)

    # Assert
    assert result is None


def test_handle_skip_existing_file_should_skip(tmp_path, mock_extraction_history):
    """Test _handle_skip_existing_file when file exists and should skip."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )
    request = build_request(tmp_path)
    request.replace = False
    output_path = tmp_path / "output.csv"
    output_path.write_text("existing data")

    # Act
    result = manager._handle_skip_existing_file(request, output_path, 0.0)

    # Assert
    assert result is not None
    assert result.success is True
    assert result.output_file == output_path


def test_setup_progress_tracker_single_chunk(tmp_path, mock_extraction_history):
    """Test _setup_progress_tracker with single chunk (no progress tracker)."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )
    request = build_request(tmp_path)
    request.date_range.end = request.date_range.start  # Same time = single chunk

    # Act
    progress_tracker, strategy, use_chunking = manager._setup_progress_tracker(request, None)

    # Assert
    assert progress_tracker is None
    assert strategy is not None
    assert isinstance(use_chunking, bool)


def test_setup_progress_tracker_multiple_chunks(tmp_path, mock_extraction_history):
    """Test _setup_progress_tracker with multiple chunks."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )
    request = build_request(tmp_path)
    # Request has 10 minute range with 15 minute chunks, but let's force multiple chunks
    from phasor_point_cli.chunk_strategy import ChunkStrategy

    strategy = ChunkStrategy(chunk_size_minutes=5, logger=MagicMock())

    # Act
    _progress_tracker, returned_strategy, _use_chunking = manager._setup_progress_tracker(
        request, strategy
    )

    # Assert - with 10 minute range and 5 minute chunks, should get progress tracker
    assert returned_strategy == strategy


def test_process_and_calculate_successful(tmp_path, mock_extraction_history):
    """Test _process_and_calculate with successful processing."""
    # Arrange
    df = pd.DataFrame({"ts": [1, 2, 3], "value": [1, 2, 3]})
    processor = MagicMock()
    processor.process.return_value = (df, [])

    power_calculator = MagicMock()
    power_calculator.process_phasor_data.return_value = (df, None)

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        data_processor=processor,
        power_calculator=power_calculator,
        extraction_history=mock_extraction_history,
    )

    request = build_request(tmp_path)
    extraction_log = {}

    # Act
    result_df, failure_result = manager._process_and_calculate(df, request, extraction_log, 0.0)

    # Assert
    assert result_df is not None
    assert failure_result is None
    processor.process.assert_called_once()
    power_calculator.process_phasor_data.assert_called_once()


def test_process_and_calculate_processing_fails(tmp_path, mock_extraction_history):
    """Test _process_and_calculate when processing returns None."""
    # Arrange
    df = pd.DataFrame({"ts": [1, 2, 3], "value": [1, 2, 3]})
    processor = MagicMock()
    processor.process.return_value = (None, [])

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        data_processor=processor,
        extraction_history=mock_extraction_history,
    )

    request = build_request(tmp_path)
    extraction_log = {}

    # Act
    result_df, failure_result = manager._process_and_calculate(df, request, extraction_log, 0.0)

    # Assert
    assert result_df is None
    assert failure_result is not None
    assert failure_result.success is False
    assert failure_result.error is not None
    assert "Data processing returned no data" in failure_result.error


def test_process_and_calculate_power_calc_fails(tmp_path, mock_extraction_history):
    """Test _process_and_calculate when power calculation returns None."""
    # Arrange
    df = pd.DataFrame({"ts": [1, 2, 3], "value": [1, 2, 3]})
    processor = MagicMock()
    processor.process.return_value = (df, [])

    power_calculator = MagicMock()
    power_calculator.process_phasor_data.return_value = (None, None)

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        data_processor=processor,
        power_calculator=power_calculator,
        extraction_history=mock_extraction_history,
    )

    request = build_request(tmp_path)
    extraction_log = {}

    # Act
    result_df, failure_result = manager._process_and_calculate(df, request, extraction_log, 0.0)

    # Assert
    assert result_df is None
    assert failure_result is not None
    assert failure_result.error is not None
    assert "Power calculation returned no data" in failure_result.error


def test_process_and_calculate_skips_when_not_needed(tmp_path, mock_extraction_history):
    """Test _process_and_calculate skips processing when clean=False and processed=False."""
    # Arrange
    df = pd.DataFrame({"ts": [1, 2, 3], "value": [1, 2, 3]})
    processor = MagicMock()
    power_calculator = MagicMock()

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        data_processor=processor,
        power_calculator=power_calculator,
        extraction_history=mock_extraction_history,
    )

    request = build_request(tmp_path)
    request.clean = False
    request.processed = False
    extraction_log = {}

    # Act
    result_df, failure_result = manager._process_and_calculate(df, request, extraction_log, 0.0)

    # Assert
    assert result_df is df
    assert failure_result is None
    processor.process.assert_not_called()
    power_calculator.process_phasor_data.assert_not_called()


def test_resolve_batch_output_dir_explicit(tmp_path, mock_extraction_history):
    """Test _resolve_batch_output_dir with explicit output_dir."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )
    explicit_dir = tmp_path / "custom_output"

    # Act
    result = manager._resolve_batch_output_dir(explicit_dir)

    # Assert
    assert result == explicit_dir
    assert result.exists()


def test_resolve_batch_output_dir_from_config(tmp_path, mock_extraction_history):
    """Test _resolve_batch_output_dir with config default."""
    # Arrange
    config = {"output": {"default_output_dir": str(tmp_path / "config_output")}}
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub(config),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )

    # Act
    result = manager._resolve_batch_output_dir(None)

    # Assert
    assert "config_output" in str(result)
    assert result.exists()


def test_resolve_batch_output_dir_fallback(tmp_path, mock_extraction_history):
    """Test _resolve_batch_output_dir with no config (uses fallback)."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )

    # Act
    result = manager._resolve_batch_output_dir(None)

    # Assert
    assert "data_exports" in str(result)
    assert result.exists()


def test_handle_batch_cancellation(tmp_path, mock_extraction_history):
    """Test _handle_batch_cancellation creates cancelled results."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )
    requests = [build_request(tmp_path) for _ in range(5)]

    # Act - cancel after processing 2
    results = manager._handle_batch_cancellation(requests, 2)

    # Assert
    assert len(results) == 3  # Remaining 3 requests
    assert all(r.success is False for r in results)
    assert all(r.error == "Extraction cancelled by user" for r in results)
    assert all(r.rows_extracted == 0 for r in results)


def test_print_batch_summary_all_successful(tmp_path, mock_extraction_history):
    """Test _print_batch_summary with all successful."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )

    request = build_request(tmp_path)
    results = [
        ExtractionResult(
            request=request,
            success=True,
            output_file=tmp_path / "output.csv",
            rows_extracted=100,
            extraction_time_seconds=5.0,
        )
        for _ in range(3)
    ]

    from phasor_point_cli.models import BatchExtractionResult

    batch_result = BatchExtractionResult(
        batch_id="test_batch",
        results=results,
        started_at=datetime.now(),
        finished_at=datetime.now(),
    )

    cancellation_manager = MagicMock()
    cancellation_manager.is_cancelled.return_value = False

    # Act
    manager._print_batch_summary(batch_result, cancellation_manager)

    # Assert
    manager.logger.info.assert_called()


def test_print_batch_summary_with_failures(tmp_path, mock_extraction_history):
    """Test _print_batch_summary with failures."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )

    request = build_request(tmp_path)
    results = [
        ExtractionResult(
            request=request,
            success=True,
            output_file=tmp_path / "output.csv",
            rows_extracted=100,
            extraction_time_seconds=5.0,
        ),
        ExtractionResult(
            request=request,
            success=False,
            output_file=None,
            rows_extracted=0,
            extraction_time_seconds=0.0,
            error="Test error",
        ),
    ]

    from phasor_point_cli.models import BatchExtractionResult

    batch_result = BatchExtractionResult(
        batch_id="test_batch",
        results=results,
        started_at=datetime.now(),
        finished_at=datetime.now(),
    )

    cancellation_manager = MagicMock()
    cancellation_manager.is_cancelled.return_value = False

    # Act
    manager._print_batch_summary(batch_result, cancellation_manager)

    # Assert
    manager.logger.error.assert_called()


def test_print_batch_summary_with_cancellation(tmp_path, mock_extraction_history):
    """Test _print_batch_summary with cancellation."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub({}),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )

    request = build_request(tmp_path)
    results = [
        ExtractionResult(
            request=request,
            success=False,
            output_file=None,
            rows_extracted=0,
            extraction_time_seconds=0.0,
            error="Extraction cancelled by user",
        ),
    ]

    from phasor_point_cli.models import BatchExtractionResult

    batch_result = BatchExtractionResult(
        batch_id="test_batch",
        results=results,
        started_at=datetime.now(),
        finished_at=datetime.now(),
    )

    cancellation_manager = MagicMock()
    cancellation_manager.is_cancelled.return_value = True

    # Act
    manager._print_batch_summary(batch_result, cancellation_manager)

    # Assert
    # Should log cancellation info
    assert any("cancelled" in str(call).lower() for call in manager.logger.info.call_args_list)


def test_single_precheck_skips_when_file_exists_without_replace(
    tmp_path, mock_extraction_history, monkeypatch
):
    """Test that single extract skips when file exists at expected path without replace flag."""
    # Arrange - Change to tmp_path directory so files are created there
    monkeypatch.chdir(tmp_path)

    df = pd.DataFrame({"ts": [1, 2, 3], "value": [1, 2, 3]})
    extractor = MagicMock()
    extractor.extract.return_value = df

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub(
            {"available_pmus": [{"id": 45012, "station_name": "PMU A", "country": "NO"}]}
        ),
        logger=MagicMock(),
        data_extractor=extractor,
        extraction_history=mock_extraction_history,
    )

    # Build request
    date_range = DateRange(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 0, 10, 0),
    )
    request = ExtractionRequest(
        pmu_id=45012,
        date_range=date_range,
        resolution=1,
        processed=True,
        clean=True,
        output_format="csv",
        replace=False,
    )

    # Create the expected file
    expected_path = manager._expected_output_path(request)
    expected_path.write_text("existing data")

    # Act
    result = manager.extract(request)

    # Assert
    assert result.success is True
    assert result.output_file == expected_path
    extractor.extract.assert_not_called()  # Should skip extraction


def test_batch_precheck_skips_when_file_exists_without_replace(tmp_path, mock_extraction_history):
    """Test that batch extract skips when file exists at expected path without replace flag."""
    # Arrange
    df = pd.DataFrame({"ts": [1, 2, 3], "value": [1, 2, 3]})
    extractor = MagicMock()
    extractor.extract.return_value = df

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub(
            {"available_pmus": [{"id": 45012, "station_name": "PMU A", "country": "NO"}]}
        ),
        logger=MagicMock(),
        data_extractor=extractor,
        extraction_history=mock_extraction_history,
    )

    # Build request
    date_range = DateRange(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 0, 10, 0),
    )
    request = ExtractionRequest(
        pmu_id=45012,
        date_range=date_range,
        resolution=1,
        processed=True,
        clean=True,
        output_format="csv",
        replace=False,
    )

    # Create the expected file in output_dir
    output_dir = tmp_path / "batch_output"
    output_dir.mkdir()
    expected_path = manager._expected_output_path(request, output_dir)
    expected_path.write_text("existing data")

    # Act
    result = manager.extract(request, output_dir=output_dir)

    # Assert
    assert result.success is True
    assert result.output_file == expected_path
    extractor.extract.assert_not_called()  # Should skip extraction


def test_overwrites_when_replace_true(tmp_path, mock_extraction_history, monkeypatch):
    """Test that extraction proceeds and overwrites when replace=True."""
    # Arrange - Change to tmp_path directory so files are created there
    monkeypatch.chdir(tmp_path)

    df = pd.DataFrame({"ts": [1, 2, 3], "value": [1, 2, 3]})
    extractor = MagicMock()
    extractor.extract.return_value = df

    processor = MagicMock()
    processor.process.return_value = (df, [])

    power_calculator = MagicMock()
    power_calculator.process_phasor_data.return_value = (df, None)

    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub(
            {"available_pmus": [{"id": 45012, "station_name": "PMU A", "country": "NO"}]}
        ),
        logger=MagicMock(),
        data_extractor=extractor,
        data_processor=processor,
        power_calculator=power_calculator,
        extraction_history=mock_extraction_history,
    )

    # Build request
    date_range = DateRange(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 0, 10, 0),
    )
    request = ExtractionRequest(
        pmu_id=45012,
        date_range=date_range,
        resolution=1,
        processed=True,
        clean=True,
        output_format="csv",
        replace=True,
    )

    # Create the expected file
    expected_path = manager._expected_output_path(request)
    expected_path.write_text("old data")

    # Act
    result = manager.extract(request)

    # Assert
    assert result.success is True
    assert result.output_file == expected_path
    extractor.extract.assert_called_once()  # Should proceed with extraction
    assert expected_path.exists()
    # File should be overwritten with new data
    assert "old data" not in expected_path.read_text()


def test_unified_filename_single_vs_batch(tmp_path, mock_extraction_history):
    """Test that single and batch produce identical base filenames for same request."""
    # Arrange
    manager = ExtractionManager(
        connection_pool=None,
        config_manager=ConfigStub(
            {"available_pmus": [{"id": 45012, "station_name": "PMU A", "country": "NO"}]}
        ),
        logger=MagicMock(),
        extraction_history=mock_extraction_history,
    )

    # Build request
    date_range = DateRange(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 0, 10, 0),
    )
    request = ExtractionRequest(
        pmu_id=45012,
        date_range=date_range,
        resolution=1,
        processed=True,
        clean=True,
        output_format="csv",
        replace=False,
    )

    # Act
    single_path = manager._expected_output_path(request, output_dir=None)
    batch_dir = tmp_path / "batch_output"
    batch_path = manager._expected_output_path(request, output_dir=batch_dir)

    # Assert
    # Base filenames should be identical
    assert single_path.name == batch_path.name
    # Full paths differ only by directory
    assert batch_path.parent == batch_dir
    # Single path should be just a filename with no directory component
    assert single_path == Path(single_path.name)
    assert str(single_path.parent) == "."
