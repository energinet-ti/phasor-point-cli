"""
Unit tests for the ExtractionManager class.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from phasor_point_cli.extraction_history import ExtractionHistory
from phasor_point_cli.extraction_manager import ExtractionManager
from phasor_point_cli.models import DateRange, ExtractionRequest


class ConfigStub:
    def __init__(self, data):
        self.config = data

    def get_pmu_info(self, pmu_id):
        """Get PMU info from config."""
        from phasor_point_cli.models import PMUInfo

        for region, pmus in self.config.get("available_pmus", {}).items():
            for pmu_data in pmus:
                if pmu_data["id"] == pmu_id:
                    return PMUInfo(
                        id=pmu_data["id"],
                        station_name=pmu_data["station_name"],
                        region=region,
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
    config = {"available_pmus": {"RegionA": [{"number": 45012, "name": "PMU A", "country": "NO"}]}}

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
        "available_pmus": {
            "RegionA": [
                {"id": 45012, "station_name": "PMU A", "country": "NO"},
                {"id": 45013, "station_name": "PMU B", "country": "NO"},
            ]
        }
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
        "available_pmus": {
            "RegionA": [
                {"id": 45012, "station_name": "PMU A", "country": "NO"},
                {"id": 45013, "station_name": "PMU B", "country": "NO"},
            ]
        }
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
    config = {
        "available_pmus": {"RegionA": [{"id": 45012, "station_name": "PMU A", "country": "NO"}]}
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

    request = build_request(tmp_path)

    # Mock json.dump to raise error
    import json

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
    config = {
        "available_pmus": {"RegionA": [{"id": 45012, "station_name": "PMU A", "country": "NO"}]}
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
        skip_existing=True,  # Try to read log
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
        "available_pmus": {
            "RegionA": [
                {"id": 45012, "station_name": "PMU A", "country": "NO"},
                {"id": 45013, "station_name": "PMU B", "country": "NO"},
            ]
        }
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
    # Arrange
    monkeypatch.setenv("TZ", "Invalid/Timezone")

    # Act & Assert
    with pytest.warns(
        UserWarning,
        match="Invalid timezone in TZ environment variable: 'Invalid/Timezone'",
    ):
        result = ExtractionManager._get_local_timezone()

    # Should still return a timezone (system fallback)
    assert result is not None


def test_get_utc_offset_invalid_timezone_warns():
    """Test that UTC offset calculation failure issues warning."""
    # Arrange
    dt = datetime(2024, 7, 15, 10, 0, 0)
    invalid_tz = "not_a_timezone_object"  # Will cause attribute errors

    # Act & Assert
    with pytest.warns(
        UserWarning,
        match="Failed to calculate UTC offset.*Defaulting to \\+00:00",
    ):
        result = ExtractionManager._get_utc_offset(dt, invalid_tz)

    # Should return default offset
    assert result == "+00:00"
