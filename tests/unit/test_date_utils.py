"""
Unit tests for DateRangeCalculator class.
"""

import argparse
from datetime import datetime, timedelta

import pytest

from phasor_point_cli.date_utils import DateRangeCalculator


class TestDateRangeCalculator:
    """Test suite for DateRangeCalculator class."""

    def test_calculate_absolute_range(self):
        """Test calculation with absolute start and end dates."""
        args = argparse.Namespace(
            start="2025-01-01 00:00:00",
            end="2025-01-01 12:00:00",
            minutes=None,
            hours=None,
            days=None,
        )

        result = DateRangeCalculator.calculate(args)

        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 1, 12, 0, 0)
        assert result.batch_timestamp is None
        assert result.is_relative is False

    def test_calculate_minutes_backward(self):
        """Test calculation with minutes (backward from now)."""
        reference = datetime(2025, 1, 1, 12, 0, 0)
        args = argparse.Namespace(start=None, end=None, minutes=60, hours=None, days=None)

        result = DateRangeCalculator.calculate(args, reference_time=reference)

        assert result.start == datetime(2025, 1, 1, 11, 0, 0)
        assert result.end == datetime(2025, 1, 1, 12, 0, 0)
        assert result.batch_timestamp == "20250101_120000"
        assert result.is_relative is True

    def test_calculate_hours_backward(self):
        """Test calculation with hours (backward from now)."""
        reference = datetime(2025, 1, 1, 12, 0, 0)
        args = argparse.Namespace(start=None, end=None, minutes=None, hours=2, days=None)

        result = DateRangeCalculator.calculate(args, reference_time=reference)

        assert result.start == datetime(2025, 1, 1, 10, 0, 0)
        assert result.end == datetime(2025, 1, 1, 12, 0, 0)
        assert result.batch_timestamp == "20250101_120000"
        assert result.is_relative is True

    def test_calculate_days_backward(self):
        """Test calculation with days (backward from now)."""
        reference = datetime(2025, 1, 5, 12, 0, 0)
        args = argparse.Namespace(start=None, end=None, minutes=None, hours=None, days=2)

        result = DateRangeCalculator.calculate(args, reference_time=reference)

        assert result.start == datetime(2025, 1, 3, 12, 0, 0)
        assert result.end == datetime(2025, 1, 5, 12, 0, 0)
        assert result.batch_timestamp == "20250105_120000"
        assert result.is_relative is True

    def test_calculate_start_with_minutes_forward(self):
        """Test calculation with start + minutes (forward)."""
        args = argparse.Namespace(
            start="2025-01-01 00:00:00", end=None, minutes=30, hours=None, days=None
        )

        result = DateRangeCalculator.calculate(args)

        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 1, 0, 30, 0)
        assert result.batch_timestamp == "20250101_000000"
        assert result.is_relative is False

    def test_calculate_start_with_hours_forward(self):
        """Test calculation with start + hours (forward)."""
        args = argparse.Namespace(
            start="2025-01-01 00:00:00", end=None, minutes=None, hours=3, days=None
        )

        result = DateRangeCalculator.calculate(args)

        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 1, 3, 0, 0)
        assert result.batch_timestamp == "20250101_000000"
        assert result.is_relative is False

    def test_calculate_start_with_days_forward(self):
        """Test calculation with start + days (forward)."""
        args = argparse.Namespace(
            start="2025-01-01 00:00:00", end=None, minutes=None, hours=None, days=1
        )

        result = DateRangeCalculator.calculate(args)

        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 2, 0, 0, 0)
        assert result.batch_timestamp == "20250101_000000"
        assert result.is_relative is False

    def test_calculate_missing_args(self):
        """Test calculation with missing required arguments."""
        args = argparse.Namespace(start=None, end=None, minutes=None, hours=None, days=None)

        with pytest.raises(ValueError, match="Please specify either"):
            DateRangeCalculator.calculate(args)

    def test_calculate_from_duration(self):
        """Test calculation from duration in minutes."""
        reference = datetime(2025, 1, 1, 12, 0, 0)

        result = DateRangeCalculator.calculate_from_duration(
            duration_minutes=120, reference_time=reference
        )

        assert result.start == datetime(2025, 1, 1, 10, 0, 0)
        assert result.end == datetime(2025, 1, 1, 12, 0, 0)
        assert result.batch_timestamp == "20250101_120000"
        assert result.is_relative is True

    def test_calculate_from_duration_default_reference(self):
        """Test calculation from duration with default reference time."""
        result = DateRangeCalculator.calculate_from_duration(duration_minutes=60)

        # Should calculate from now
        assert result.is_relative is True
        assert result.batch_timestamp is not None
        assert (result.end - result.start) == timedelta(minutes=60)

    def test_calculate_from_start_and_duration(self):
        """Test calculation from start date and duration."""
        result = DateRangeCalculator.calculate_from_start_and_duration(
            start_date="2025-01-01 00:00:00", duration=timedelta(hours=2)
        )

        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 1, 2, 0, 0)
        assert result.batch_timestamp == "20250101_000000"
        assert result.is_relative is False

    def test_calculate_priority_start_duration_over_absolute(self):
        """Test that start+duration takes priority over absolute range."""
        args = argparse.Namespace(
            start="2025-01-01 00:00:00",
            end="2025-01-01 23:59:59",  # Should be ignored
            minutes=30,
            hours=None,
            days=None,
        )

        result = DateRangeCalculator.calculate(args)

        # Should use start + 30 minutes, not start + end
        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 1, 0, 30, 0)

    def test_calculate_priority_duration_over_absolute(self):
        """Test that duration takes priority over absolute range when start is missing."""
        args = argparse.Namespace(
            start=None,
            end="2025-01-01 23:59:59",  # Should be ignored
            minutes=60,
            hours=None,
            days=None,
        )

        reference = datetime(2025, 1, 1, 12, 0, 0)
        result = DateRangeCalculator.calculate(args, reference_time=reference)

        # Should use 60 minutes backward from reference
        assert result.start == datetime(2025, 1, 1, 11, 0, 0)
        assert result.end == datetime(2025, 1, 1, 12, 0, 0)
