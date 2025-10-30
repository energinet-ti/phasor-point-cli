"""
Unit tests for the DataExtractor class.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from phasor_point_cli.chunk_strategy import ChunkStrategy
from phasor_point_cli.data_extractor import DataExtractor
from phasor_point_cli.models import DateRange, ExtractionRequest


def ts(timestamp_str: str) -> pd.Timestamp:
    """Helper to create a Timestamp with proper type annotation for tests."""
    result = pd.Timestamp(timestamp_str)
    assert isinstance(result, pd.Timestamp), f"Failed to create timestamp from {timestamp_str}"
    return result


def build_request(start, end, **overrides):
    date_range = DateRange(start=start, end=end)
    return ExtractionRequest(
        pmu_id=45012,
        date_range=date_range,
        output_file=None,
        resolution=1,
        processed=True,
        clean=True,
        chunk_size_minutes=overrides.get("chunk_size_minutes", 15),
        parallel_workers=overrides.get("parallel_workers", 1),
        output_format="parquet",
    )


@pytest.fixture
def connection_pool():
    pool = MagicMock()
    pool.get_connection.return_value = MagicMock()
    return pool


def test_extract_single_query_returns_dataframe(connection_pool, mocker):
    # Arrange
    start = datetime(2025, 1, 1, 12, 0, 0)
    end = datetime(2025, 1, 1, 12, 5, 0)
    request = build_request(start, end, chunk_size_minutes=30)
    expected_df = pd.DataFrame(
        {"ts": pd.date_range(start, periods=3, freq="1min"), "value": [1, 2, 3]}
    )
    mocker.patch.object(DataExtractor, "_read_dataframe", return_value=expected_df)
    extractor = DataExtractor(connection_pool=connection_pool, logger=mocker.Mock())

    # Act
    result = extractor.extract(request)

    # Assert
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    connection_pool.get_connection.assert_called_once()
    connection_pool.return_connection.assert_called_once()


def test_extract_chunked_sequential_merges_chunks(connection_pool, mocker):
    # Arrange
    start = datetime(2025, 1, 1, 0, 0, 0)
    end = datetime(2025, 1, 1, 1, 0, 0)
    request = build_request(start, end, chunk_size_minutes=30, parallel_workers=1)
    frames = [
        pd.DataFrame({"ts": [pd.Timestamp("2025-01-01 00:00:00")], "value": [1]}),
        pd.DataFrame({"ts": [pd.Timestamp("2025-01-01 00:30:00")], "value": [2]}),
    ]
    mocker.patch.object(DataExtractor, "_read_dataframe", side_effect=frames)
    extractor = DataExtractor(connection_pool=connection_pool, logger=mocker.Mock())

    # Act
    result = extractor.extract(request)

    # Assert
    assert result is not None
    assert len(result) == 2
    assert list(result["value"]) == [1, 2]
    assert connection_pool.get_connection.call_count == 2


def test_extract_chunked_parallel_combines_results(connection_pool, mocker):
    # Arrange
    start = datetime(2025, 1, 1, 0, 0, 0)
    end = datetime(2025, 1, 1, 1, 0, 0)
    request = build_request(start, end, chunk_size_minutes=30, parallel_workers=2)

    def fake_chunk(self, table_name, chunk_start, chunk_end, index):
        df = pd.DataFrame({"ts": [chunk_start], "value": [index]})
        return df, None, {"total_time": 0}

    mocker.patch.object(
        DataExtractor, "_extract_single_chunk", side_effect=fake_chunk, autospec=True
    )
    extractor = DataExtractor(connection_pool=connection_pool, logger=mocker.Mock())

    # Act
    result = extractor.extract(request)

    # Assert
    assert result is not None
    assert len(result) == 2
    assert sorted(result["value"].tolist()) == [0, 1]


def test_combine_chunks_removes_duplicate_timestamps(connection_pool, mocker):
    # Arrange
    extractor = DataExtractor(connection_pool=connection_pool, logger=mocker.Mock())
    chunk_list = [
        (0, pd.DataFrame({"ts": [pd.Timestamp("2025-01-01 00:00:00")], "value": [1]})),
        (1, pd.DataFrame({"ts": [pd.Timestamp("2025-01-01 00:00:00")], "value": [2]})),
    ]

    # Act
    combined = extractor.combine_chunks(chunk_list)

    # Assert
    assert combined is not None
    assert len(combined) == 1
    assert combined.iloc[0]["value"] == 1


# ================================================================ Error Paths ==
def test_extract_single_no_connection(connection_pool, mocker):
    """Test extract_single when connection pool returns None."""
    # Arrange
    connection_pool.get_connection.return_value = None
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    # Act
    result = extractor.extract_single("pmu_45012_1", "2025-01-01 00:00:00", "2025-01-01 01:00:00")

    # Assert
    assert result is None


def test_extract_single_query_returns_none(connection_pool, mocker):
    """Test extract_single when query returns None."""
    # Arrange
    mocker.patch.object(DataExtractor, "_read_dataframe", return_value=None)
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    # Act
    result = extractor.extract_single("pmu_45012_1", "2025-01-01 00:00:00", "2025-01-01 01:00:00")

    # Assert
    assert result is None
    logger.error.assert_called()


def test_extract_single_empty_dataframe(connection_pool, mocker):
    """Test extract_single when query returns empty dataframe."""
    # Arrange
    empty_df = pd.DataFrame()
    mocker.patch.object(DataExtractor, "_read_dataframe", return_value=empty_df)
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    # Act
    result = extractor.extract_single("pmu_45012_1", "2025-01-01 00:00:00", "2025-01-01 01:00:00")

    # Assert
    assert result is None
    logger.error.assert_called()
    # Check that the tip message was printed
    assert any("No data found" in str(call) for call in logger.error.call_args_list)


def test_extract_single_exception_handling(connection_pool, mocker):
    """Test extract_single exception handling."""
    # Arrange
    mocker.patch.object(DataExtractor, "_read_dataframe", side_effect=Exception("Database error"))
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    # Act
    result = extractor.extract_single("pmu_45012_1", "2025-01-01 00:00:00", "2025-01-01 01:00:00")

    # Assert
    assert result is None
    logger.error.assert_called()
    logger.debug.assert_called()  # Traceback logged to debug
    connection_pool.return_connection.assert_called_once()


def test_extract_sequential_no_connection_for_chunk(connection_pool, mocker):
    """Test sequential extraction when connection fails for a chunk."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    # First connection succeeds, second fails
    conn_mock = MagicMock()
    connection_pool.get_connection.side_effect = [conn_mock, None]

    df1 = pd.DataFrame({"ts": [pd.Timestamp("2025-01-01 00:00:00")], "value": [1]})
    mocker.patch.object(DataExtractor, "_read_dataframe", return_value=df1)

    chunks = [
        (ts("2025-01-01 00:00:00"), ts("2025-01-01 00:30:00")),
        (ts("2025-01-01 00:30:00"), ts("2025-01-01 01:00:00")),
    ]

    # Act
    result = extractor.extract_chunk_sequential("pmu_45012_1", chunks)

    # Assert
    assert len(result) == 1  # Only first chunk succeeded
    logger.error.assert_called()
    assert "Could not create connection" in str(logger.error.call_args)


def test_extract_sequential_chunk_exception(connection_pool, mocker):
    """Test sequential extraction when a chunk raises an exception."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    # First chunk succeeds, second raises exception
    df1 = pd.DataFrame({"ts": [pd.Timestamp("2025-01-01 00:00:00")], "value": [1]})
    mocker.patch.object(
        DataExtractor, "_read_dataframe", side_effect=[df1, Exception("Query error")]
    )

    chunks = [
        (ts("2025-01-01 00:00:00"), ts("2025-01-01 00:30:00")),
        (ts("2025-01-01 00:30:00"), ts("2025-01-01 01:00:00")),
    ]

    # Act
    result = extractor.extract_chunk_sequential("pmu_45012_1", chunks)

    # Assert
    assert len(result) == 1  # Only first chunk succeeded
    logger.error.assert_called()
    assert "Error processing chunk" in str(logger.error.call_args)


def test_extract_sequential_empty_chunk(connection_pool, mocker):
    """Test sequential extraction when a chunk returns empty data."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    df1 = pd.DataFrame({"ts": [pd.Timestamp("2025-01-01 00:00:00")], "value": [1]})
    empty_df = pd.DataFrame()
    mocker.patch.object(DataExtractor, "_read_dataframe", side_effect=[df1, empty_df])

    chunks = [
        (ts("2025-01-01 00:00:00"), ts("2025-01-01 00:30:00")),
        (ts("2025-01-01 00:30:00"), ts("2025-01-01 01:00:00")),
    ]

    # Act
    result = extractor.extract_chunk_sequential("pmu_45012_1", chunks)

    # Assert
    assert len(result) == 1  # Only first chunk had data
    logger.warning.assert_called()
    assert "No data found for chunk" in str(logger.warning.call_args)


# ============================================================ Timing/Parallel ==
def test_extract_single_chunk_success(connection_pool, mocker):
    """Test _extract_single_chunk returns dataframe and timing info."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    df = pd.DataFrame({"ts": [pd.Timestamp("2025-01-01 00:00:00")], "value": [1]})
    mocker.patch.object(DataExtractor, "_read_dataframe", return_value=df)

    # Act
    result_df, error, timing = extractor._extract_single_chunk(
        "pmu_45012_1", ts("2025-01-01 00:00:00"), ts("2025-01-01 00:30:00"), 0
    )

    # Assert
    assert result_df is not None
    assert error is None
    assert "connection_time" in timing
    assert "query_time" in timing
    assert "total_time" in timing
    assert len(result_df) == 1


def test_extract_single_chunk_no_connection(connection_pool, mocker):
    """Test _extract_single_chunk when connection fails."""
    # Arrange
    connection_pool.get_connection.return_value = None
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    # Act
    result_df, error, timing = extractor._extract_single_chunk(
        "pmu_45012_1", ts("2025-01-01 00:00:00"), ts("2025-01-01 00:30:00"), 0
    )

    # Assert
    assert result_df is None
    assert error is not None
    assert "Could not create connection" in error
    assert "connection_time" in timing


def test_extract_single_chunk_no_data(connection_pool, mocker):
    """Test _extract_single_chunk when query returns no data."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    empty_df = pd.DataFrame()
    mocker.patch.object(DataExtractor, "_read_dataframe", return_value=empty_df)

    # Act
    result_df, error, timing = extractor._extract_single_chunk(
        "pmu_45012_1", ts("2025-01-01 00:00:00"), ts("2025-01-01 00:30:00"), 0
    )

    # Assert
    assert result_df is None
    assert error is not None
    assert "No data found" in error
    assert "query_time" in timing


def test_extract_single_chunk_exception(connection_pool, mocker):
    """Test _extract_single_chunk exception handling."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    mocker.patch.object(DataExtractor, "_read_dataframe", side_effect=Exception("Query failed"))

    # Act
    result_df, error, timing = extractor._extract_single_chunk(
        "pmu_45012_1", ts("2025-01-01 00:00:00"), ts("2025-01-01 00:30:00"), 0
    )

    # Assert
    assert result_df is None
    assert error is not None
    assert "Error processing chunk" in error
    assert "total_time" in timing
    connection_pool.return_connection.assert_called_once()


def test_extract_chunk_with_timing_wrapper(connection_pool, mocker):
    """Test extract_chunk_with_timing public wrapper."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    df = pd.DataFrame({"ts": [pd.Timestamp("2025-01-01 00:00:00")], "value": [1]})
    mocker.patch.object(DataExtractor, "_read_dataframe", return_value=df)

    # Act
    result_df, error, timing = extractor.extract_chunk_with_timing(
        "pmu_45012_1", ts("2025-01-01 00:00:00"), ts("2025-01-01 00:30:00"), 0
    )

    # Assert
    assert result_df is not None
    assert error is None
    assert timing is not None


def test_extract_parallel_with_failed_chunks(connection_pool, mocker):
    """Test parallel extraction logs warnings for failed chunks."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    # First chunk succeeds, second fails
    def mock_single_chunk(self, table_name, chunk_start, chunk_end, idx):
        if idx == 0:
            return pd.DataFrame({"ts": [chunk_start], "value": [1]}), None, {"total_time": 0.1}
        return None, "Chunk failed", {"total_time": 0.1}

    mocker.patch.object(
        DataExtractor, "_extract_single_chunk", side_effect=mock_single_chunk, autospec=True
    )

    chunks = [
        (ts("2025-01-01 00:00:00"), ts("2025-01-01 00:30:00")),
        (ts("2025-01-01 00:30:00"), ts("2025-01-01 01:00:00")),
    ]

    # Act
    result = extractor.extract_chunk_parallel("pmu_45012_1", chunks, parallel_workers=2)

    # Assert
    assert len(result) == 1  # Only successful chunk returned
    logger.warning.assert_called()
    assert "failed" in str(logger.warning.call_args)


# =========================================================== Combine Chunks ==
def test_combine_chunks_empty_list(connection_pool, mocker):
    """Test combine_chunks with empty chunk list."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    # Act
    result = extractor.combine_chunks([])

    # Assert
    assert result is None
    logger.warning.assert_called()
    assert "No chunk data to combine" in str(logger.warning.call_args)


def test_combine_chunks_all_none(connection_pool, mocker):
    """Test combine_chunks with all None chunks."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    # Act
    result = extractor.combine_chunks([None, None, None])

    # Assert
    assert result is None
    logger.warning.assert_called()


def test_combine_chunks_all_empty_dataframes(connection_pool, mocker):
    """Test combine_chunks with all empty dataframes."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    chunk_list = [
        (0, pd.DataFrame()),
        (1, pd.DataFrame()),
    ]

    # Act
    result = extractor.combine_chunks(chunk_list)

    # Assert
    assert result is None
    logger.warning.assert_called()


def test_combine_chunks_without_tuples(connection_pool, mocker):
    """Test combine_chunks with plain dataframes (not tuples)."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    chunk_list = [
        pd.DataFrame({"ts": [pd.Timestamp("2025-01-01 00:00:00")], "value": [1]}),
        pd.DataFrame({"ts": [pd.Timestamp("2025-01-01 00:30:00")], "value": [2]}),
    ]

    # Act
    result = extractor.combine_chunks(chunk_list)

    # Assert
    assert result is not None
    assert len(result) == 2
    assert list(result["value"]) == [1, 2]


def test_combine_chunks_without_ts_column(connection_pool, mocker):
    """Test combine_chunks with dataframes lacking 'ts' column."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    chunk_list = [
        pd.DataFrame({"value": [1]}),
        pd.DataFrame({"value": [2]}),
    ]

    # Act
    result = extractor.combine_chunks(chunk_list)

    # Assert
    assert result is not None
    assert len(result) == 2
    # Should not try to sort/deduplicate without ts column


# ======================================================== Strategy Management ==
def test_ensure_strategy_creates_new_when_needed(connection_pool, mocker):
    """Test _ensure_strategy creates new strategy when chunk size changes."""
    # Arrange
    logger = mocker.Mock()
    initial_strategy = ChunkStrategy(chunk_size_minutes=30, logger=logger)
    extractor = DataExtractor(
        connection_pool=connection_pool, logger=logger, chunk_strategy=initial_strategy
    )

    # Act
    new_strategy = extractor._ensure_strategy(chunk_size_minutes=60)

    # Assert
    assert new_strategy is not initial_strategy
    assert new_strategy.chunk_size_minutes == 60
    assert extractor.chunk_strategy is new_strategy


def test_ensure_strategy_reuses_when_same(connection_pool, mocker):
    """Test _ensure_strategy reuses existing strategy when chunk size matches."""
    # Arrange
    logger = mocker.Mock()
    initial_strategy = ChunkStrategy(chunk_size_minutes=30, logger=logger)
    extractor = DataExtractor(
        connection_pool=connection_pool, logger=logger, chunk_strategy=initial_strategy
    )

    # Act
    same_strategy = extractor._ensure_strategy(chunk_size_minutes=30)

    # Assert
    assert same_strategy is initial_strategy


# ================================================================ Helpers ==
def test_read_dataframe_suppresses_warnings(connection_pool, mocker):
    """Test _read_dataframe suppresses SQLAlchemy warnings."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)
    conn_mock = MagicMock()

    expected_df = pd.DataFrame({"value": [1, 2, 3]})

    with patch("pandas.read_sql", return_value=expected_df) as mock_read_sql:
        # Act
        result = extractor._read_dataframe(conn_mock, "SELECT * FROM test")

        # Assert
        assert result is not None
        assert len(result) == 3
        mock_read_sql.assert_called_once_with("SELECT * FROM test", conn_mock)


def test_build_query_format(connection_pool, mocker):
    """Test _build_query creates correct SQL format with half-open interval [start, end)."""
    # Arrange
    logger = mocker.Mock()
    extractor = DataExtractor(connection_pool=connection_pool, logger=logger)

    # Act
    query = extractor._build_query("pmu_45012_1", "2025-01-01 00:00:00", "2025-01-01 01:00:00")

    # Assert
    assert "SELECT *" in query
    assert "FROM pmu_45012_1" in query
    assert "WHERE ts >=" in query
    assert "AND ts <" in query
    assert "2025-01-01 00:00:00" in query
    assert "2025-01-01 01:00:00" in query
    assert "ORDER BY ts" in query
