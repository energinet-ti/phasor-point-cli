"""
Chunking strategy utilities for the refactored extraction pipeline.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd


class ChunkStrategy:
    """Determine whether chunking is needed and generate chunk ranges."""

    def __init__(self, chunk_size_minutes: int = 5, logger=None):
        if chunk_size_minutes <= 0:
            raise ValueError("chunk_size_minutes must be positive")

        self.chunk_size_minutes = chunk_size_minutes
        self.logger = logger

    def should_use_chunking(
        self, start_date, end_date
    ) -> tuple[bool, list[tuple[pd.Timestamp, pd.Timestamp]]]:
        """Return whether chunking is required and the resulting ranges."""
        chunks = self.create_chunks(start_date, end_date)
        use_chunking = len(chunks) > 1
        return use_chunking, chunks

    def create_chunks(self, start_date, end_date) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
        """Create contiguous chunks covering the requested date range."""
        try:
            start_dt = self._to_timestamp(start_date)
            end_dt = self._to_timestamp(end_date)
        except Exception as exc:
            if self.logger:
                self.logger.warning(f"Error parsing dates for chunking: {exc}")
            raise

        if end_dt < start_dt:
            raise ValueError("end_date must be greater than or equal to start_date")

        chunk_delta = pd.Timedelta(minutes=self.chunk_size_minutes)
        if (end_dt - start_dt) <= chunk_delta:
            return [(start_dt, end_dt)]

        chunks: list[tuple[pd.Timestamp, pd.Timestamp]] = []
        current_start = start_dt
        while current_start < end_dt:
            chunk_end: pd.Timestamp = min(current_start + chunk_delta, end_dt)  # type: ignore[assignment]
            chunks.append((current_start, chunk_end))
            current_start = chunk_end

        return chunks

    def estimate_chunk_count(self, start_date, end_date) -> int:
        """Return the number of chunks that would be produced."""
        return len(self.create_chunks(start_date, end_date))

    @staticmethod
    def _to_timestamp(value) -> pd.Timestamp:
        """Convert common date inputs to pandas Timestamp."""
        if isinstance(value, pd.Timestamp):
            return value
        if isinstance(value, datetime):
            ts_result = pd.Timestamp(value)
            if isinstance(ts_result, pd.Timestamp):
                return ts_result
            raise ValueError(f"Could not convert datetime {value} to timestamp")
        result = pd.to_datetime(value)
        if pd.isna(result):
            raise ValueError(f"Could not convert {value} to timestamp")
        if not isinstance(result, pd.Timestamp):
            raise ValueError(f"Conversion resulted in {type(result)}, expected Timestamp")
        return result
