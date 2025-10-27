"""
Unit tests for the DataProcessor class.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import pytz

from phasor_point_cli.data_processor import DataProcessor
from phasor_point_cli.models import DataQualityThresholds


class DummyConfigManager:
    def get_data_quality_thresholds(self):
        return DataQualityThresholds(
            frequency_min=49, frequency_max=51, null_threshold_percent=40, gap_multiplier=3
        )


@pytest.fixture
def extraction_log():
    return {
        "column_changes": {"removed": [], "type_conversions": []},
        "issues_found": [],
        "data_quality": {},
        "statistics": {},
    }


def test_format_timestamps_with_precision_handles_multiple_columns():
    # Arrange
    df = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2025-01-01 12:00:00.123456"]),
            "ts_local": pd.to_datetime(["2025-01-01 11:00:00.654321"]),
        }
    )

    # Act
    result = DataProcessor.format_timestamps_with_precision(df, ["ts", "ts_local"])

    # Assert
    assert result["ts"].iloc[0].endswith("123")
    assert result["ts_local"].iloc[0].endswith("654")


def test_convert_columns_to_numeric_logs_conversion(extraction_log):
    # Arrange
    df = pd.DataFrame(
        {"ts": pd.date_range("2025-01-01", periods=3, freq="1s"), "value": ["1", "invalid", "3"]}
    )
    logger = MagicMock()

    # Act
    converted = DataProcessor.convert_columns_to_numeric(df.copy(), extraction_log, logger)

    # Assert
    assert pd.api.types.is_numeric_dtype(converted["value"])
    assert extraction_log["column_changes"]["type_conversions"][0]["column"] == "value"
    logger.info.assert_called()


def test_clean_and_convert_types_applies_timezone_and_numeric(extraction_log):
    # Arrange
    df = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2025-01-01 12:00:00", "2025-01-01 12:01:00"]),
            "value": ["1", "2"],
        }
    )
    processor = DataProcessor(logger=MagicMock())

    # Act
    with patch.object(DataProcessor, "get_local_timezone", return_value=pytz.timezone("UTC")):
        result = processor.clean_and_convert_types(df.copy(), extraction_log)

    # Assert
    assert result is not None
    assert "ts_local" in result.columns
    assert pd.api.types.is_numeric_dtype(result["value"])


def test_process_with_validation_updates_issues(extraction_log):
    # Arrange
    df = pd.DataFrame(
        {
            "ts": pd.date_range("2025-01-01", periods=4, freq="1s"),
            "value": [1, None, 3, None],
            "f": [48, 49, 50, 52],
        }
    )
    processor = DataProcessor(config_manager=DummyConfigManager(), logger=MagicMock())  # type: ignore[arg-type]

    # Act
    with patch.object(DataProcessor, "get_local_timezone", return_value=pytz.timezone("UTC")):
        processed_df, issues = processor.process(
            df.copy(), extraction_log, clean=True, validate=True
        )

    # Assert
    assert processed_df is not None
    assert isinstance(issues, list)
    assert extraction_log["data_quality"]["validation_summary"]["issues_found"] == len(issues)


def test_process_without_clean_skips_cleaning(extraction_log):
    # Arrange
    df = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2025-01-01 12:00:00", "2025-01-01 12:01:00"]),
            "value": ["1", "2"],
        }
    )
    processor = DataProcessor(config_manager=DummyConfigManager(), logger=MagicMock())  # type: ignore[arg-type]

    # Act
    with patch.object(DataProcessor, "clean_and_convert_types") as mock_clean:
        processor.process(df.copy(), extraction_log, clean=False, validate=False)

    # Assert
    mock_clean.assert_not_called()


class TestDSTProcessing:
    """Test suite for DST-aware data processing."""

    def test_timezone_conversion_across_dst_transition(self, extraction_log, monkeypatch):
        """Test that timezone conversion handles DST transitions correctly."""
        monkeypatch.setenv("TZ", "Europe/Copenhagen")

        # Create dataframe with UTC timestamps spanning DST transition
        # October 27, 2024: Copenhagen switches from CEST (UTC+2) to CET (UTC+1) at 03:00 local
        df = pd.DataFrame(
            {
                "ts": pd.to_datetime(
                    [
                        "2024-10-27 00:00:00",  # UTC 00:00 → 02:00 CEST (before transition)
                        "2024-10-27 00:30:00",  # UTC 00:30 → 02:30 CEST (before transition)
                        "2024-10-27 01:00:00",  # UTC 01:00 → 03:00 CEST (at transition)
                        "2024-10-27 02:00:00",  # UTC 02:00 → 03:00 CET (after transition)
                    ]
                )
            }
        )

        processor = DataProcessor(logger=MagicMock())

        # Act
        result = processor.apply_timezone_conversion(df.copy(), extraction_log)

        # Assert
        assert "ts_local" in result.columns
        assert "ts" in result.columns

        # ts should remain as UTC (unchanged)
        assert "2024-10-27" in str(result["ts"].iloc[0])
        assert "00:00" in str(result["ts"].iloc[0])

        # ts_local should show local time with correct DST offset for each timestamp
        # Note: After conversion, times may be formatted as strings
        assert result["ts_local"].iloc[0] is not None
        assert result["ts_local"].iloc[-1] is not None

        # Verify DST transition is detected in extraction log
        assert "timestamp_adjustment" in extraction_log["data_quality"]
        assert extraction_log["data_quality"]["timestamp_adjustment"]["dst_transition"] is True

    def test_summer_time_conversion(self, extraction_log, monkeypatch):
        """Test timezone conversion during summer (DST active)."""
        monkeypatch.setenv("TZ", "Europe/Copenhagen")

        # Create dataframe with summer UTC timestamps
        df = pd.DataFrame(
            {
                "ts": pd.to_datetime(
                    [
                        "2024-07-15 08:00:00",  # UTC 08:00 → 10:00 CEST (UTC+2)
                        "2024-07-15 09:00:00",  # UTC 09:00 → 11:00 CEST
                        "2024-07-15 10:00:00",  # UTC 10:00 → 12:00 CEST
                    ]
                )
            }
        )

        processor = DataProcessor(logger=MagicMock())

        # Act
        result = processor.apply_timezone_conversion(df.copy(), extraction_log)

        # Assert
        assert "ts_local" in result.columns
        assert "ts" in result.columns

        # ts should remain as UTC (unchanged)
        assert "2024-07-15" in str(result["ts"].iloc[0])
        assert "08:00" in str(result["ts"].iloc[0])

        # ts_local should show CEST times (UTC+2)
        assert "10:00" in str(result["ts_local"].iloc[0]) or "10.00" in str(
            result["ts_local"].iloc[0]
        )

    def test_winter_time_conversion(self, extraction_log, monkeypatch):
        """Test timezone conversion during winter (DST inactive)."""
        monkeypatch.setenv("TZ", "Europe/Copenhagen")

        # Create dataframe with winter UTC timestamps
        df = pd.DataFrame(
            {
                "ts": pd.to_datetime(
                    [
                        "2024-01-15 09:00:00",  # UTC 09:00 → 10:00 CET (UTC+1)
                        "2024-01-15 10:00:00",  # UTC 10:00 → 11:00 CET
                        "2024-01-15 11:00:00",  # UTC 11:00 → 12:00 CET
                    ]
                )
            }
        )

        processor = DataProcessor(logger=MagicMock())

        # Act
        result = processor.apply_timezone_conversion(df.copy(), extraction_log)

        # Assert
        assert "ts_local" in result.columns
        assert "ts" in result.columns

        # ts should remain as UTC (unchanged)
        assert "2024-01-15" in str(result["ts"].iloc[0])
        assert "09:00" in str(result["ts"].iloc[0])

        # ts_local should show CET times (UTC+1)
        assert "10:00" in str(result["ts_local"].iloc[0]) or "10.00" in str(
            result["ts_local"].iloc[0]
        )

    def test_timezone_conversion_logs_offset(self, extraction_log, monkeypatch):
        """Test that timezone conversion logs offset information."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")
        df = pd.DataFrame({"ts": pd.to_datetime(["2024-07-15 10:00:00"])})
        processor = DataProcessor(logger=MagicMock())

        # Act
        processor.apply_timezone_conversion(df.copy(), extraction_log)

        # Assert
        assert "timestamp_adjustment" in extraction_log["data_quality"]
        assert "timezone" in extraction_log["data_quality"]["timestamp_adjustment"]
        assert "offset_hours_start" in extraction_log["data_quality"]["timestamp_adjustment"]
        assert "offset_hours_end" in extraction_log["data_quality"]["timestamp_adjustment"]
        assert "dst_transition" in extraction_log["data_quality"]["timestamp_adjustment"]
        assert (
            extraction_log["data_quality"]["timestamp_adjustment"]["method"] == "per_row_dst_aware"
        )
