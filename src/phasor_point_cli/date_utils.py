"""
Date utility functions for PhasorPoint CLI.

Provides utilities for calculating date ranges from various input formats including
relative durations and absolute timestamps.
"""

from __future__ import annotations

import os
import warnings
from datetime import datetime, timedelta

import pandas as pd
import pytz
import tzlocal

from .models import DateRange


class DateRangeCalculator:
    """Calculates date ranges from command arguments."""

    @staticmethod
    def _parse_local_datetime(date_string: str) -> datetime:
        """
        Parse a date string as user's local time.

        Args:
            date_string: Date string to parse (e.g., "2024-07-15 10:00:00")

        Returns:
            Naive datetime representing user's local time input
        """
        return pd.to_datetime(date_string).to_pydatetime()

    @staticmethod
    def get_local_timezone():
        """Get system's local timezone, preferring TZ environment variable."""
        tz_env = os.environ.get("TZ")
        if tz_env:
            try:
                return pytz.timezone(tz_env)
            except pytz.exceptions.UnknownTimeZoneError:
                warnings.warn(
                    f"Invalid timezone in TZ environment variable: '{tz_env}'. "
                    f"Falling back to system timezone.",
                    UserWarning,
                    stacklevel=2,
                )

        try:
            detected_tz = tzlocal.get_localzone()
            tz_name = str(detected_tz)
            if tz_name and not tz_name.startswith("UTC"):
                try:
                    return pytz.timezone(tz_name)
                except Exception:
                    return detected_tz
            return detected_tz
        except Exception:
            return pytz.UTC

    @staticmethod
    def convert_to_database_time(local_dt: datetime) -> datetime:
        """
        Convert user's local time to database time (CET, UTC+1 fixed, no DST).

        Args:
            local_dt: Naive datetime in user's local timezone

        Returns:
            Naive datetime in database timezone (UTC+1) for SQL queries
        """
        local_tz = DateRangeCalculator.get_local_timezone()

        # Localize to system's local timezone
        if local_tz and hasattr(local_tz, "localize"):
            aware_dt = local_tz.localize(local_dt, is_dst=True)  # type: ignore[attr-defined]
        else:
            aware_dt = local_dt.replace(tzinfo=local_tz if local_tz else pytz.UTC)

        # Convert to UTC
        utc_dt = aware_dt.astimezone(pytz.UTC)

        # Convert to database timezone (UTC+1 fixed, no DST)
        db_offset = timedelta(hours=1)
        db_dt = utc_dt + db_offset

        # Return as naive datetime for query
        return db_dt.replace(tzinfo=None)

    @staticmethod
    def get_utc_offset(local_dt: datetime) -> str:
        """
        Get UTC offset string for a datetime in user's local timezone.

        Args:
            local_dt: Naive datetime in user's local timezone

        Returns:
            Offset string in format "+HH:MM" or "-HH:MM"
        """
        try:
            local_tz = DateRangeCalculator.get_local_timezone()
            if local_tz is None:
                return "+00:00"

            # Localize the naive datetime to get timezone-aware version
            if hasattr(local_tz, "localize"):
                aware_dt = local_tz.localize(local_dt, is_dst=True)  # type: ignore[attr-defined]
            else:
                aware_dt = local_dt.replace(tzinfo=local_tz)

            # Get offset in seconds
            offset_seconds = aware_dt.utcoffset().total_seconds() if aware_dt.utcoffset() else 0
            offset_hours = int(offset_seconds // 3600)
            offset_minutes = int((abs(offset_seconds) % 3600) // 60)

            sign = "+" if offset_seconds >= 0 else "-"
            return f"{sign}{abs(offset_hours):02d}:{offset_minutes:02d}"
        except Exception as e:
            warnings.warn(
                f"Failed to calculate UTC offset for datetime {local_dt}: {e}. "
                f"Defaulting to +00:00. Check timezone configuration.",
                UserWarning,
                stacklevel=2,
            )
            return "+00:00"

    @staticmethod
    def calculate(args, reference_time: datetime | None = None) -> DateRange:
        """
        Calculate start and end dates based on command arguments.

        Supports multiple formats:
        - Absolute: --start + --end
        - Relative (backward): --minutes/--hours/--days (from reference_time)
        - Relative (forward): --start + --minutes/--hours/--days

        Args:
            args: Parsed command-line arguments with start, end, minutes, hours, days
            reference_time: Reference datetime for relative calculations (default: now)

        Returns:
            DateRange with start_date, end_date, and batch_timestamp

        Raises:
            ValueError: If date range arguments are invalid or missing

        Examples:
            >>> args = argparse.Namespace(minutes=60, start=None, end=None, hours=None, days=None)
            >>> result = DateRangeCalculator.calculate(args)
            >>> # Returns date range for last 60 minutes
        """
        if reference_time is None:
            reference_time = datetime.now()

        # Priority: --start + duration, then duration alone, then --start + --end
        if getattr(args, "start", None) and DateRangeCalculator._has_duration(args):
            # --start with duration: start at given time and go forward
            start_dt = DateRangeCalculator._parse_local_datetime(args.start)
            duration = DateRangeCalculator._extract_duration(args)
            end_dt = start_dt + duration
            return DateRange(start=start_dt, end=end_dt)

        if DateRangeCalculator._has_duration(args):
            # Duration alone: go back N minutes/hours/days from now
            duration = DateRangeCalculator._extract_duration(args)
            end_dt = reference_time
            start_dt = end_dt - duration
            return DateRange(start=start_dt, end=end_dt)

        if getattr(args, "start", None) and getattr(args, "end", None):
            # Absolute time range
            start_dt = DateRangeCalculator._parse_local_datetime(args.start)
            end_dt = DateRangeCalculator._parse_local_datetime(args.end)
            return DateRange(start=start_dt, end=end_dt)

        raise ValueError("Please specify either --start/--end dates, --minutes, --hours, or --days")

    @staticmethod
    def calculate_from_duration(
        duration_minutes: int, reference_time: datetime | None = None
    ) -> DateRange:
        """
        Calculate date range from duration in minutes.

        Creates a date range going backward from reference_time.

        Args:
            duration_minutes: Duration in minutes
            reference_time: End time for the range (default: now)

        Returns:
            DateRange going backward from reference_time

        Examples:
            >>> result = DateRangeCalculator.calculate_from_duration(60)
            >>> # Returns range for last 60 minutes
        """
        if reference_time is None:
            reference_time = datetime.now()

        end_dt = reference_time
        start_dt = end_dt - timedelta(minutes=duration_minutes)
        return DateRange(start=start_dt, end=end_dt)

    @staticmethod
    def calculate_from_start_and_duration(start_date: str, duration: timedelta) -> DateRange:
        """
        Calculate date range from start date and duration.

        Creates a date range going forward from start_date.

        Args:
            start_date: Start date as string (parseable by pandas)
            duration: Duration as timedelta

        Returns:
            DateRange going forward from start_date

        Examples:
            >>> result = DateRangeCalculator.calculate_from_start_and_duration(
            ...     "2025-01-01 00:00:00",
            ...     timedelta(hours=1)
            ... )
        """
        start_dt = DateRangeCalculator._parse_local_datetime(start_date)
        end_dt = start_dt + duration
        return DateRange(start=start_dt, end=end_dt)

    @staticmethod
    def _has_duration(args) -> bool:
        """Check if args contain any duration specification."""
        return bool(
            getattr(args, "minutes", None)
            or getattr(args, "hours", None)
            or getattr(args, "days", None)
        )

    @staticmethod
    def _extract_duration(args) -> timedelta:
        """Extract timedelta from args duration fields."""
        if getattr(args, "minutes", None):
            return timedelta(minutes=args.minutes)
        if getattr(args, "hours", None):
            return timedelta(hours=args.hours)
        if getattr(args, "days", None):
            return timedelta(days=args.days)
        raise ValueError("No duration specified in args")
