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

        # DateRange stores user's input time
        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 1, 12, 0, 0)

        # Conversion to database time adds 1 hour (UTC → UTC+1)
        db_start, db_end = result.as_database_time()
        assert db_start == datetime(2025, 1, 1, 1, 0, 0)
        assert db_end == datetime(2025, 1, 1, 13, 0, 0)

    def test_calculate_minutes_backward(self):
        """Test calculation with minutes (backward from now)."""
        reference = datetime(2025, 1, 1, 12, 0, 0)
        args = argparse.Namespace(start=None, end=None, minutes=60, hours=None, days=None)

        result = DateRangeCalculator.calculate(args, reference_time=reference)

        assert result.start == datetime(2025, 1, 1, 11, 0, 0)
        assert result.end == datetime(2025, 1, 1, 12, 0, 0)

    def test_calculate_hours_backward(self):
        """Test calculation with hours (backward from now)."""
        reference = datetime(2025, 1, 1, 12, 0, 0)
        args = argparse.Namespace(start=None, end=None, minutes=None, hours=2, days=None)

        result = DateRangeCalculator.calculate(args, reference_time=reference)

        assert result.start == datetime(2025, 1, 1, 10, 0, 0)
        assert result.end == datetime(2025, 1, 1, 12, 0, 0)

    def test_calculate_days_backward(self):
        """Test calculation with days (backward from now)."""
        reference = datetime(2025, 1, 5, 12, 0, 0)
        args = argparse.Namespace(start=None, end=None, minutes=None, hours=None, days=2)

        result = DateRangeCalculator.calculate(args, reference_time=reference)

        assert result.start == datetime(2025, 1, 3, 12, 0, 0)
        assert result.end == datetime(2025, 1, 5, 12, 0, 0)

    def test_calculate_start_with_minutes_forward(self, monkeypatch):
        """Test calculation with start + minutes (forward)."""
        monkeypatch.setenv("TZ", "UTC")

        args = argparse.Namespace(
            start="2025-01-01 00:00:00", end=None, minutes=30, hours=None, days=None
        )

        result = DateRangeCalculator.calculate(args)

        # DateRange stores user's input time
        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 1, 0, 30, 0)

        # Conversion to database time adds 1 hour (UTC → UTC+1)
        db_start, db_end = result.as_database_time()
        assert db_start == datetime(2025, 1, 1, 1, 0, 0)
        assert db_end == datetime(2025, 1, 1, 1, 30, 0)

    def test_calculate_start_with_hours_forward(self, monkeypatch):
        """Test calculation with start + hours (forward)."""
        monkeypatch.setenv("TZ", "UTC")

        args = argparse.Namespace(
            start="2025-01-01 00:00:00", end=None, minutes=None, hours=3, days=None
        )

        result = DateRangeCalculator.calculate(args)

        # DateRange stores user's input time
        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 1, 3, 0, 0)

        # Conversion to database time adds 1 hour (UTC → UTC+1)
        db_start, db_end = result.as_database_time()
        assert db_start == datetime(2025, 1, 1, 1, 0, 0)
        assert db_end == datetime(2025, 1, 1, 4, 0, 0)

    def test_calculate_start_with_days_forward(self, monkeypatch):
        """Test calculation with start + days (forward)."""
        monkeypatch.setenv("TZ", "UTC")

        args = argparse.Namespace(
            start="2025-01-01 00:00:00", end=None, minutes=None, hours=None, days=1
        )

        result = DateRangeCalculator.calculate(args)

        # DateRange stores user's input time
        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 2, 0, 0, 0)

        # Conversion to database time adds 1 hour (UTC → UTC+1)
        db_start, db_end = result.as_database_time()
        assert db_start == datetime(2025, 1, 1, 1, 0, 0)
        assert db_end == datetime(2025, 1, 2, 1, 0, 0)

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

    def test_calculate_from_duration_default_reference(self):
        """Test calculation from duration with default reference time."""
        result = DateRangeCalculator.calculate_from_duration(duration_minutes=60)

        # Should calculate from now
        assert (result.end - result.start) == timedelta(minutes=60)

    def test_calculate_from_start_and_duration(self, monkeypatch):
        """Test calculation from start date and duration."""
        monkeypatch.setenv("TZ", "UTC")

        result = DateRangeCalculator.calculate_from_start_and_duration(
            start_date="2025-01-01 00:00:00", duration=timedelta(hours=2)
        )

        # DateRange stores user's input time
        assert result.start == datetime(2025, 1, 1, 0, 0, 0)
        assert result.end == datetime(2025, 1, 1, 2, 0, 0)

        # Conversion to database time adds 1 hour (UTC → UTC+1)
        db_start, db_end = result.as_database_time()
        assert db_start == datetime(2025, 1, 1, 1, 0, 0)
        assert db_end == datetime(2025, 1, 1, 3, 0, 0)

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
        """Test parsing a summer date converts to database timezone (UTC+1 fixed)."""
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
        # DateRange stores user's input time
        assert result.start == datetime(2024, 7, 15, 10, 0, 0)
        assert result.end == datetime(2024, 7, 15, 11, 0, 0)

        # Copenhagen summer (CEST) = UTC+2
        # 10:00 CEST → 08:00 UTC → 09:00 database time (UTC+1)
        db_start, db_end = result.as_database_time()
        assert db_start == datetime(2024, 7, 15, 9, 0, 0)
        assert db_end == datetime(2024, 7, 15, 10, 0, 0)

    def test_parse_winter_date_in_copenhagen_timezone(self, monkeypatch):
        """Test parsing a winter date when system and database timezones match."""
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
        # DateRange stores user's input time
        assert result.start == datetime(2024, 1, 15, 10, 0, 0)
        assert result.end == datetime(2024, 1, 15, 11, 0, 0)

        # Copenhagen winter (CET) = UTC+1, database = UTC+1
        # 10:00 CET → 09:00 UTC → 10:00 database time (UTC+1)
        # No change since both are UTC+1
        db_start, db_end = result.as_database_time()
        assert db_start == datetime(2024, 1, 15, 10, 0, 0)
        assert db_end == datetime(2024, 1, 15, 11, 0, 0)

    def test_parse_summer_date_requested_in_winter(self, monkeypatch):
        """Test that summer dates use summer DST offset for conversion."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")
        args = argparse.Namespace(
            start="2024-07-15 14:00:00",
            end="2024-07-15 15:00:00",
            minutes=None,
            hours=None,
            days=None,
        )

        # Act
        result = DateRangeCalculator.calculate(args)

        # Assert
        # DateRange stores user's input time
        assert result.start == datetime(2024, 7, 15, 14, 0, 0)
        assert result.end == datetime(2024, 7, 15, 15, 0, 0)

        # 14:00 CEST (UTC+2) → 12:00 UTC → 13:00 database (UTC+1)
        db_start, db_end = result.as_database_time()
        assert db_start == datetime(2024, 7, 15, 13, 0, 0)
        assert db_end == datetime(2024, 7, 15, 14, 0, 0)

    def test_parse_winter_date_requested_in_summer(self, monkeypatch):
        """Test that winter dates use winter DST offset for conversion."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")
        args = argparse.Namespace(
            start="2024-12-15 14:00:00",
            end="2024-12-15 15:00:00",
            minutes=None,
            hours=None,
            days=None,
        )

        # Act
        result = DateRangeCalculator.calculate(args)

        # Assert
        # DateRange stores user's input time
        assert result.start == datetime(2024, 12, 15, 14, 0, 0)
        assert result.end == datetime(2024, 12, 15, 15, 0, 0)

        # 14:00 CET (UTC+1) → 13:00 UTC → 14:00 database (UTC+1)
        # No change since both are UTC+1
        db_start, db_end = result.as_database_time()
        assert db_start == datetime(2024, 12, 15, 14, 0, 0)
        assert db_end == datetime(2024, 12, 15, 15, 0, 0)

    def test_parse_ambiguous_time_during_fall_back(self, monkeypatch):
        """Test parsing ambiguous time during DST fall-back transition."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")
        # In Copenhagen, DST ends last Sunday of October at 03:00 (becomes 02:00)
        # 2024-10-27 02:30:00 occurs twice - we use first occurrence (DST active)
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
        # DateRange stores user's input time
        assert result.start == datetime(2024, 10, 27, 2, 30, 0)
        assert result.end == datetime(2024, 10, 27, 2, 45, 0)

        # First occurrence: 02:30 CEST (UTC+2) → 00:30 UTC → 01:30 database (UTC+1)
        db_start, db_end = result.as_database_time()
        assert db_start == datetime(2024, 10, 27, 1, 30, 0)
        assert db_end == datetime(2024, 10, 27, 1, 45, 0)

    def test_parse_spring_forward_gap(self, monkeypatch):
        """Test parsing during spring forward gap (non-existent times)."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")
        # In Copenhagen, DST starts last Sunday of March at 02:00 (becomes 03:00)
        # 2024-03-31 02:30:00 doesn't exist in local time
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
        """Test calculate_from_start_and_duration converts to database timezone."""
        # Arrange
        monkeypatch.setenv("TZ", "Europe/Copenhagen")

        # Act
        result = DateRangeCalculator.calculate_from_start_and_duration(
            start_date="2024-07-15 10:00:00",
            duration=timedelta(hours=2),
        )

        # Assert
        # DateRange stores user's input time
        assert result.start == datetime(2024, 7, 15, 10, 0, 0)
        assert result.end == datetime(2024, 7, 15, 12, 0, 0)

        # 10:00 CEST (UTC+2) → 08:00 UTC → 09:00 database (UTC+1)
        # Duration: 2 hours → End: 11:00 database time
        db_start, db_end = result.as_database_time()
        assert db_start == datetime(2024, 7, 15, 9, 0, 0)
        assert db_end == datetime(2024, 7, 15, 11, 0, 0)

    def test_utc_timezone_parsing(self, monkeypatch):
        """Test that UTC timezone converts to database timezone."""
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
        # DateRange stores user's input time
        assert result.start == datetime(2024, 7, 15, 10, 0, 0)
        assert result.end == datetime(2024, 7, 15, 11, 0, 0)

        # 10:00 UTC → 11:00 database (UTC+1)
        db_start, db_end = result.as_database_time()
        assert db_start == datetime(2024, 7, 15, 11, 0, 0)
        assert db_end == datetime(2024, 7, 15, 12, 0, 0)

    def test_invalid_timezone_warns_and_falls_back(self, monkeypatch):
        """Test that invalid TZ environment variable falls back gracefully."""
        # Arrange
        monkeypatch.setenv("TZ", "Invalid/Timezone")
        args = argparse.Namespace(
            start="2024-07-15 10:00:00",
            end="2024-07-15 11:00:00",
            minutes=None,
            hours=None,
            days=None,
        )

        # Act
        result = DateRangeCalculator.calculate(args)

        # Assert - should warn when converting to database time
        with pytest.warns(UserWarning, match="Invalid timezone in TZ environment variable"):
            db_start, db_end = result.as_database_time()

        # Should still convert successfully (falls back to system timezone)
        assert db_start is not None
        assert db_end is not None
