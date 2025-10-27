"""
Unit tests for DateRangeCalculator class.
"""

import argparse
from datetime import datetime, timedelta

import pytest

from phasor_point_cli.date_utils import DateRangeCalculator


class TestDateRangeCalculator:
    """Test suite for DateRangeCalculator class."""

    def test_calculate_absolute_range(self, monkeypatch):
        """Test calculation with absolute start and end dates."""
        monkeypatch.setenv("TZ", "UTC")

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

    def test_calculate_start_with_minutes_forward(self, monkeypatch):
        """Test calculation with start + minutes (forward)."""
        monkeypatch.setenv("TZ", "UTC")

        args = argparse.Namespace(
            start="2025-01-01 00:00:00", end=None, minutes=30, hours=None, days=None
        )

        result = DateRangeCalculator.calculate(args)

        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 1, 0, 30, 0)
        assert result.batch_timestamp == "20250101_000000"
        assert result.is_relative is False

    def test_calculate_start_with_hours_forward(self, monkeypatch):
        """Test calculation with start + hours (forward)."""
        monkeypatch.setenv("TZ", "UTC")

        args = argparse.Namespace(
            start="2025-01-01 00:00:00", end=None, minutes=None, hours=3, days=None
        )

        result = DateRangeCalculator.calculate(args)

        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 1, 3, 0, 0)
        assert result.batch_timestamp == "20250101_000000"
        assert result.is_relative is False

    def test_calculate_start_with_days_forward(self, monkeypatch):
        """Test calculation with start + days (forward)."""
        monkeypatch.setenv("TZ", "UTC")

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

    def test_calculate_from_start_and_duration(self, monkeypatch):
        """Test calculation from start date and duration."""
        monkeypatch.setenv("TZ", "UTC")

        result = DateRangeCalculator.calculate_from_start_and_duration(
            start_date="2025-01-01 00:00:00", duration=timedelta(hours=2)
        )

        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 1, 2, 0, 0)
        assert result.batch_timestamp == "20250101_000000"
        assert result.is_relative is False

    def test_calculate_priority_start_duration_over_absolute(self, monkeypatch):
        """Test that start+duration takes priority over absolute range."""
        monkeypatch.setenv("TZ", "UTC")

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


class TestDSTHandling:
    """Test suite for DST-aware date parsing."""

    def test_parse_summer_date_in_copenhagen_timezone(self, monkeypatch):
        """Test parsing a summer date with DST active (Copenhagen uses CEST in summer)."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")
        args = argparse.Namespace(
            start="2024-07-15 10:00:00",
            end="2024-07-15 11:00:00",
            minutes=None,
            hours=None,
            days=None,
        )

        # Act
        result = DateRangeCalculator.calculate(args)

        # Assert
        # Copenhagen summer time is UTC+2
        # 2024-07-15 10:00:00 CEST = 2024-07-15 08:00:00 UTC
        assert result.start == datetime(2024, 7, 15, 8, 0, 0)
        assert result.end == datetime(2024, 7, 15, 9, 0, 0)

    def test_parse_winter_date_in_copenhagen_timezone(self, monkeypatch):
        """Test parsing a winter date with DST inactive (Copenhagen uses CET in winter)."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")
        args = argparse.Namespace(
            start="2024-01-15 10:00:00",
            end="2024-01-15 11:00:00",
            minutes=None,
            hours=None,
            days=None,
        )

        # Act
        result = DateRangeCalculator.calculate(args)

        # Assert
        # Copenhagen winter time is UTC+1
        # 2024-01-15 10:00:00 CET = 2024-01-15 09:00:00 UTC
        assert result.start == datetime(2024, 1, 15, 9, 0, 0)
        assert result.end == datetime(2024, 1, 15, 10, 0, 0)

    def test_parse_summer_date_requested_in_winter(self, monkeypatch):
        """Test that summer dates are correctly parsed even when requested during winter."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")
        # Simulate making request in winter (doesn't affect parsing)
        args = argparse.Namespace(
            start="2024-07-15 14:00:00",  # Summer date
            end="2024-07-15 15:00:00",
            minutes=None,
            hours=None,
            days=None,
        )

        # Act
        result = DateRangeCalculator.calculate(args)

        # Assert
        # Should still parse as summer time (UTC+2), not winter time (UTC+1)
        # 2024-07-15 14:00:00 CEST = 2024-07-15 12:00:00 UTC
        assert result.start == datetime(2024, 7, 15, 12, 0, 0)
        assert result.end == datetime(2024, 7, 15, 13, 0, 0)

    def test_parse_winter_date_requested_in_summer(self, monkeypatch):
        """Test that winter dates are correctly parsed even when requested during summer."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")
        # Simulate making request in summer (doesn't affect parsing)
        args = argparse.Namespace(
            start="2024-12-15 14:00:00",  # Winter date
            end="2024-12-15 15:00:00",
            minutes=None,
            hours=None,
            days=None,
        )

        # Act
        result = DateRangeCalculator.calculate(args)

        # Assert
        # Should parse as winter time (UTC+1), not summer time (UTC+2)
        # 2024-12-15 14:00:00 CET = 2024-12-15 13:00:00 UTC
        assert result.start == datetime(2024, 12, 15, 13, 0, 0)
        assert result.end == datetime(2024, 12, 15, 14, 0, 0)

    def test_parse_ambiguous_time_during_fall_back(self, monkeypatch):
        """Test parsing ambiguous time during DST fall-back transition."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")
        # In Copenhagen, DST ends last Sunday of October at 03:00 (becomes 02:00)
        # 2024-10-27 02:30:00 occurs twice - we should get the first occurrence (DST active)
        args = argparse.Namespace(
            start="2024-10-27 02:30:00",
            end="2024-10-27 02:45:00",
            minutes=None,
            hours=None,
            days=None,
        )

        # Act
        result = DateRangeCalculator.calculate(args)

        # Assert
        # First occurrence: 2024-10-27 02:30:00 CEST = 2024-10-27 00:30:00 UTC
        # (with DST active, UTC+2)
        assert result.start == datetime(2024, 10, 27, 0, 30, 0)
        assert result.end == datetime(2024, 10, 27, 0, 45, 0)

    def test_parse_spring_forward_gap(self, monkeypatch):
        """Test parsing during spring forward gap (non-existent times)."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")
        # In Copenhagen, DST starts last Sunday of March at 02:00 (becomes 03:00)
        # 2024-03-31 02:30:00 doesn't exist, but pytz should handle it
        args = argparse.Namespace(
            start="2024-03-31 02:30:00",
            end="2024-03-31 03:30:00",
            minutes=None,
            hours=None,
            days=None,
        )

        # Act
        result = DateRangeCalculator.calculate(args)

        # Assert
        # pytz with is_dst=True will treat 02:30 as if DST was active
        # This is a reasonable interpretation for non-existent times
        assert result.start is not None
        assert result.end is not None

    def test_calculate_from_start_and_duration_dst_aware(self, monkeypatch):
        """Test calculate_from_start_and_duration with DST-aware parsing."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")

        # Act
        result = DateRangeCalculator.calculate_from_start_and_duration(
            start_date="2024-07-15 10:00:00",  # Summer time
            duration=timedelta(hours=2),
        )

        # Assert
        # Start: 2024-07-15 10:00:00 CEST = 08:00:00 UTC
        # Duration: 2 hours in UTC
        # End: 2024-07-15 10:00:00 UTC (naive)
        assert result.start == datetime(2024, 7, 15, 8, 0, 0)
        assert result.end == datetime(2024, 7, 15, 10, 0, 0)

    def test_utc_timezone_parsing(self, monkeypatch):
        """Test that UTC timezone works correctly."""
        # Arrange
        monkeypatch.setenv("TZ", "UTC")
        args = argparse.Namespace(
            start="2024-07-15 10:00:00",
            end="2024-07-15 11:00:00",
            minutes=None,
            hours=None,
            days=None,
        )

        # Act
        result = DateRangeCalculator.calculate(args)

        # Assert
        # UTC has no DST, so times should match exactly
        assert result.start == datetime(2024, 7, 15, 10, 0, 0)
        assert result.end == datetime(2024, 7, 15, 11, 0, 0)
