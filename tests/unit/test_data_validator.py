"""
Unit tests for the DataValidator class.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from phasor_point_cli.data_validator import DataValidator
from phasor_point_cli.models import DataQualityThresholds


@pytest.fixture
def validator():
    thresholds = DataQualityThresholds(
        frequency_min=49, frequency_max=51, null_threshold_percent=40, gap_multiplier=3
    )
    return DataValidator(thresholds)


def test_check_empty_columns_removes_and_logs(validator):
    # Arrange
    df = pd.DataFrame(
        {
            "ts": pd.date_range("2025-01-01", periods=3, freq="1s"),
            "valid": [1, 2, 3],
            "empty": [float("nan")] * 3,
        }
    )
    extraction_log = {"column_changes": {"removed": []}, "issues_found": [], "data_quality": {}}

    # Act
    cleaned, issues = validator.check_empty_columns(df, extraction_log)

    # Assert
    assert "empty" not in cleaned.columns
    assert issues == ["Empty columns: 1"]
    assert extraction_log["column_changes"]["removed"][0]["column"] == "empty"


def test_check_null_percentages_detects_high_nulls(validator):
    # Arrange
    df = pd.DataFrame(
        {
            "ts": pd.date_range("2025-01-01", periods=10, freq="1s"),
            "high_null": [1] * 4 + [None] * 6,
        }
    )

    # Act
    issues = validator.check_null_percentages(df, extraction_log=None)

    # Assert
    assert any("High null" in issue for issue in issues)


def test_check_time_continuity_identifies_gaps(validator):
    # Arrange
    timestamps = []
    base = datetime(2025, 1, 1, 12, 0, 0)
    for idx in range(5):
        timestamps.append(base + timedelta(seconds=idx))
    base = base + timedelta(seconds=60)
    for idx in range(5):
        timestamps.append(base + timedelta(seconds=idx))
    df = pd.DataFrame({"ts": timestamps, "value": range(10)})

    # Act
    issues = validator.check_time_continuity(df)

    # Assert
    assert any("Time gaps" in issue for issue in issues)


def test_check_frequency_ranges_flags_values_outside_threshold(validator):
    # Arrange
    df = pd.DataFrame(
        {
            "ts": pd.date_range("2025-01-01", periods=5, freq="1s"),
            "f": [48.0, 49.5, 50.0, 51.0, 52.0],
        }
    )

    # Act
    issues = validator.check_frequency_ranges(df)

    # Assert
    assert any("Invalid frequency values" in issue for issue in issues)


def test_validate_runs_all_checks_and_updates_extraction_log(validator):
    # Arrange
    df = pd.DataFrame(
        {
            "ts": pd.date_range("2025-01-01", periods=6, freq="1s"),
            "value": [1, None, 3, None, 5, None],
            "f": [48, 50, 50, 50, 50, 52],
        }
    )
    extraction_log = {
        "column_changes": {"removed": [], "type_conversions": []},
        "issues_found": [],
        "data_quality": {},
    }

    # Act
    _, issues = validator.validate(df, extraction_log)

    # Assert
    assert isinstance(issues, list)
    assert "thresholds" in extraction_log["data_quality"]
    assert extraction_log["data_quality"]["validation_summary"]["issues_found"] == len(issues)
