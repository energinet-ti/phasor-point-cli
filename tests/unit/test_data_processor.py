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
            "ts_utc": pd.to_datetime(["2025-01-01 11:00:00.654321"]),
        }
    )

    # Act
    result = DataProcessor.format_timestamps_with_precision(df, ["ts", "ts_utc"])

    # Assert
    assert result["ts"].iloc[0].endswith("123")
    assert result["ts_utc"].iloc[0].endswith("654")


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
    assert "ts_utc" in result.columns
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
