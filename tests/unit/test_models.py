"""
Unit tests for the dataclasses defined in phasor_point_cli.models.

These tests cover basic instantiation, validation helpers, and the lightweight
serialisation logic that will be relied upon by higher level manager classes.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from phasor_point_cli.models import (
    BatchExtractionResult,
    ChunkResult,
    DataQualityThresholds,
    DateRange,
    ExtractionRequest,
    ExtractionResult,
    PhasorColumnMap,
    PMUInfo,
    QueryResult,
    TableDiscoveryResult,
    TableInfo,
    TableStatistics,
    ValidationCheck,
    ValidationResult,
    WriteResult,
)


def test_pmu_info_instantiation_and_serialisation():
    # Arrange
    info = PMUInfo(id=45012, station_name="Test PMU", country="Finland")

    # Act
    serialised = info.to_dict()

    # Assert
    assert serialised["id"] == 45012
    assert serialised["station_name"] == "Test PMU"
    assert serialised["country"] == "Finland"


def test_pmu_info_from_dict_infers_region():
    # Arrange
    data = {"id": 45012, "station_name": "Test PMU", "country": "Sweden"}

    # Act
    info = PMUInfo.from_dict(data)

    # Assert
    assert info.country == "Sweden"
    assert info.extra_attributes == {}


def test_data_quality_thresholds_validation_success():
    # Arrange
    thresholds = DataQualityThresholds(
        frequency_min=45.0, frequency_max=65.0, null_threshold_percent=40, gap_multiplier=5
    )

    # Act
    thresholds.validate()

    # Assert
    assert thresholds.to_dict()["frequency_min"] == 45.0


@pytest.mark.parametrize(
    "kwargs, error",
    [
        (
            {
                "frequency_min": 65.0,
                "frequency_max": 45.0,
                "null_threshold_percent": 50,
                "gap_multiplier": 1,
            },
            "frequency_min",
        ),
        (
            {
                "frequency_min": 45.0,
                "frequency_max": 65.0,
                "null_threshold_percent": 150,
                "gap_multiplier": 1,
            },
            "null_threshold_percent",
        ),
        (
            {
                "frequency_min": 45.0,
                "frequency_max": 65.0,
                "null_threshold_percent": 50,
                "gap_multiplier": 0,
            },
            "gap_multiplier",
        ),
    ],
)
def test_data_quality_thresholds_validation_errors(kwargs, error):
    # Arrange
    thresholds = DataQualityThresholds(**kwargs)

    # Act & Assert
    with pytest.raises(ValueError, match=error):
        thresholds.validate()


def test_table_statistics_duration_and_serialisation():
    # Arrange
    start = datetime(2025, 1, 1, 0, 0, 0)
    end = datetime(2025, 1, 1, 1, 0, 0)
    stats = TableStatistics(
        row_count=1000, column_count=12, start_time=start, end_time=end, bytes_estimate=1024
    )

    # Act
    serialised = stats.to_dict()

    # Assert
    assert stats.duration == timedelta(hours=1)
    assert serialised["row_count"] == 1000
    assert serialised["start_time"].startswith("2025-01-01T00:00:00")


def test_table_discovery_result_serialisation():
    # Arrange
    discovery = TableDiscoveryResult(
        table_name="pmu_45012_1", pmu_id=45012, resolution=1, found=True
    )

    # Act
    payload = discovery.to_dict()

    # Assert
    assert payload["found"] is True
    assert payload["table_name"] == "pmu_45012_1"


def test_table_info_serialisation_includes_dataframe():
    # Arrange
    df = pd.DataFrame({"value": [1, 2, 3]})
    table_info = TableInfo(
        pmu_id=45012,
        resolution=1,
        table_name="pmu_45012_1",
        statistics=TableStatistics(row_count=3, column_count=1),
        sample_data=df,
    )

    # Act
    payload = table_info.to_dict()

    # Assert
    assert payload["sample_data"][0]["value"] == 1
    assert payload["statistics"]["row_count"] == 3


def test_date_range_validation_and_duration():
    # Arrange
    dr = DateRange(start=datetime(2025, 1, 1), end=datetime(2025, 1, 2))

    # Act
    dr.validate()

    # Assert
    assert dr.duration == timedelta(days=1)
    assert dr.to_strings()["start"] == "2025-01-01 00:00:00"


def test_date_range_validation_error():
    # Arrange
    dr = DateRange(start=datetime(2025, 1, 2), end=datetime(2025, 1, 1))

    # Act & Assert
    with pytest.raises(ValueError):
        dr.validate()


def test_extraction_request_validation_and_serialisation(tmp_path):
    # Arrange
    dr = DateRange(start=datetime(2025, 1, 1), end=datetime(2025, 1, 1, 1))
    request = ExtractionRequest(
        pmu_id=45012,
        date_range=dr,
        output_file=tmp_path / "output.parquet",
        resolution=1,
        processed=True,
        clean=True,
        chunk_size_minutes=30,
        parallel_workers=2,
        output_format="parquet",
    )

    # Act
    request.validate()
    payload = request.to_dict()

    # Assert
    assert payload["pmu_id"] == 45012
    assert payload["chunk_size_minutes"] == 30
    assert payload["output_file"].endswith("output.parquet")


@pytest.mark.parametrize(
    "field, value", [("resolution", 0), ("chunk_size_minutes", 0), ("parallel_workers", 0)]
)
def test_extraction_request_validation_errors(field, value):
    # Arrange
    dr = DateRange(start=datetime(2025, 1, 1), end=datetime(2025, 1, 1, 1))
    kwargs = {
        "pmu_id": 45012,
        "date_range": dr,
        "resolution": 1,
        "chunk_size_minutes": 15,
        "parallel_workers": 1,
    }
    kwargs[field] = value
    request = ExtractionRequest(**kwargs)

    # Act & Assert
    with pytest.raises(ValueError):
        request.validate()


def test_chunk_result_serialisation():
    # Arrange
    now = datetime.now(timezone.utc)
    chunk = ChunkResult(
        chunk_index=0, start=now, end=now + timedelta(minutes=1), rows=100, duration_seconds=0.5
    )

    # Act
    payload = chunk.to_dict()

    # Assert
    assert payload["rows"] == 100
    assert payload["duration_seconds"] == 0.5


def test_extraction_result_helper_methods():
    # Arrange
    dr = DateRange(start=datetime(2025, 1, 1), end=datetime(2025, 1, 1, 1))
    request = ExtractionRequest(pmu_id=45012, date_range=dr)
    chunk = ChunkResult(
        chunk_index=0, start=dr.start, end=dr.end, rows=50, duration_seconds=0.5, error=None
    )
    result = ExtractionResult(
        request=request,
        success=True,
        output_file=Path("output.parquet"),
        rows_extracted=50,
        extraction_time_seconds=1.2,
        file_size_mb=0.1,
        chunk_results=[chunk],
    )

    # Act
    payload = result.to_dict()

    # Assert
    assert result.has_errors() is False
    assert payload["rows_extracted"] == 50
    assert payload["chunks"][0]["error"] is None


def test_batch_extraction_result_grouping():
    # Arrange
    dr = DateRange(start=datetime(2025, 1, 1), end=datetime(2025, 1, 1, 1))
    request = ExtractionRequest(pmu_id=45012, date_range=dr)
    success = ExtractionResult(
        request=request,
        success=True,
        output_file=None,
        rows_extracted=10,
        extraction_time_seconds=1.0,
    )
    failure = ExtractionResult(
        request=request,
        success=False,
        output_file=None,
        rows_extracted=0,
        extraction_time_seconds=0.5,
        error="boom",
    )
    batch = BatchExtractionResult(batch_id="batch-1", results=[success, failure])

    # Act
    successful = batch.successful_results()
    failed = batch.failed_results()
    payload = batch.to_dict()

    # Assert
    assert len(successful) == 1
    assert len(failed) == 1
    assert payload["batch_id"] == "batch-1"


def test_validation_result_properties():
    # Arrange
    checks = [
        ValidationCheck(name="empty_columns", passed=True),
        ValidationCheck(name="frequency", passed=False, details="out of range"),
    ]
    result = ValidationResult(checks=checks)

    # Act
    payload = result.to_dict()

    # Assert
    assert result.is_successful is False
    assert payload["checks"][1]["details"] == "out of range"


def test_phasor_column_map_combined_columns():
    # Arrange
    column_map = PhasorColumnMap(
        voltage_magnitude={"va": "va1_m", "vb": "vb1_m"},
        current_magnitude={"ia": "ia1_m"},
        voltage_angle={"va": "va1_a"},
        current_angle={},
        frequency=["f"],
        extra_columns={"power": ["p", "q"]},
    )

    # Act
    combined = column_map.combined_columns()

    # Assert
    assert combined == ["va1_m", "vb1_m", "ia1_m", "va1_a", "f", "p", "q"]


def test_query_result_serialisation():
    # Arrange
    result = QueryResult(
        success=True, rows_returned=100, duration_seconds=0.25, output_file=Path("query.csv")
    )

    # Act
    payload = result.to_dict()

    # Assert
    assert payload["rows_returned"] == 100
    assert payload["output_file"].endswith("query.csv")


def test_write_result_serialisation():
    # Arrange
    write_result = WriteResult(
        success=True,
        output_file=Path("output.parquet"),
        file_size_mb=1.0,
        row_count=100,
        column_count=5,
        format="parquet",
    )

    # Act
    payload = write_result.to_dict()

    # Assert
    assert payload["file_size_mb"] == 1.0
    assert payload["row_count"] == 100
    assert payload["success"] is True
