"""
Unit tests for the ExtractionManager class.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

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


def build_request(tmp_path: Path) -> ExtractionRequest:
    date_range = DateRange(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 1, 0, 10, 0),
    )
    return ExtractionRequest(
        pmu_id=45012,
        date_range=date_range,
        output_file=str(tmp_path / "output.csv"),
        resolution=1,
        processed=True,
        clean=True,
        chunk_size_minutes=15,
        parallel_workers=1,
        output_format="csv",
    )


def test_extraction_manager_success(tmp_path):
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
    extractor.extract.assert_called_once_with(request, chunk_strategy=None)
    processor.process.assert_called()
    power_calculator.process_phasor_data.assert_called()


def test_extraction_manager_handles_empty_extraction(tmp_path):
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
    )
    request = build_request(tmp_path)

    # Act
    result = manager.extract(request)

    # Assert
    assert result.success is False
    assert result.output_file is None


def test_batch_extract_success(tmp_path):
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


def test_batch_extract_partial_failure(tmp_path):
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

    def mock_extract(request, chunk_strategy=None):
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
