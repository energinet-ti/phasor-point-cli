"""
Unit tests for the ChunkStrategy class.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from phasor_point_cli.chunk_strategy import ChunkStrategy


def test_chunk_strategy_single_chunk_for_small_range():
    # Arrange
    strategy = ChunkStrategy(chunk_size_minutes=30)

    # Act
    use_chunking, chunks = strategy.should_use_chunking(
        "2025-01-01 00:00:00", "2025-01-01 00:10:00"
    )

    # Assert
    assert use_chunking is False
    assert len(chunks) == 1


def test_chunk_strategy_multiple_chunks_for_large_range():
    # Arrange
    strategy = ChunkStrategy(chunk_size_minutes=15)
    start = datetime(2025, 1, 1, 0, 0, 0)
    end = datetime(2025, 1, 1, 1, 0, 0)

    # Act
    use_chunking, chunks = strategy.should_use_chunking(start, end)

    # Assert
    assert use_chunking is True
    assert len(chunks) == 4
    assert chunks[0][0] == pd.Timestamp(start)
    assert chunks[-1][1] == pd.Timestamp(end)


def test_chunk_strategy_boundary_condition_exact_multiple():
    # Arrange
    strategy = ChunkStrategy(chunk_size_minutes=30)
    start = datetime(2025, 1, 1, 0, 0)
    end = datetime(2025, 1, 1, 1, 0)

    # Act
    chunks = strategy.create_chunks(start, end)

    # Assert
    assert len(chunks) == 2
    assert chunks[0][1] - chunks[0][0] == timedelta(minutes=30)


def test_chunk_strategy_handles_strings_and_timestamps():
    # Arrange
    strategy = ChunkStrategy(chunk_size_minutes=20)
    start = "2025-01-01 00:00:00"
    end = pd.Timestamp("2025-01-01 01:00:00")

    # Act
    chunks = strategy.create_chunks(start, end)

    # Assert
    assert len(chunks) == 3


def test_chunk_strategy_estimate_chunk_count():
    # Arrange
    strategy = ChunkStrategy(chunk_size_minutes=10)

    # Act
    count = strategy.estimate_chunk_count("2025-01-01 00:00:00", "2025-01-01 00:45:00")

    # Assert
    assert count == 5


def test_chunk_strategy_invalid_range_raises():
    # Arrange
    strategy = ChunkStrategy(chunk_size_minutes=10)

    # Act & Assert
    with pytest.raises(ValueError):
        strategy.create_chunks("2025-01-01 01:00:00", "2025-01-01 00:00:00")
