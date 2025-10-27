"""
Date utility functions for PhasorPoint CLI.

Provides utilities for calculating date ranges from various input formats including
relative durations and absolute timestamps.
"""

from __future__ import annotations

import os
import warnings
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytz

from .models import DateRange


class DateRangeCalculator:
    """Calculates date ranges from command arguments."""

    @staticmethod
    def _parse_local_datetime(date_string: str) -> datetime:
        """
        Parse a date string as naive local time and return UTC datetime.

        This ensures DST is handled correctly based on the date itself, not current time.
        For example, "2024-07-15 10:00" in summer will be interpreted with summer DST offset,
        even if the current date is in winter.

        When ambiguous times occur during DST fall-back (e.g., "02:30" occurs twice),
        this method interprets them as the first occurrence (DST still active).

        Args:
            date_string: Date string to parse (e.g., "2024-07-15 10:00:00")

        Returns:
            Timezone-naive datetime in UTC (for database queries)
        """
        # Parse as naive datetime
        naive_dt = pd.to_datetime(date_string).to_pydatetime()

        # Get local timezone
        local_tz = None
        tz_env = os.environ.get("TZ")
        if tz_env:
            try:
                local_tz = pytz.timezone(tz_env)
            except pytz.exceptions.UnknownTimeZoneError:
                warnings.warn(
                    f"Invalid timezone in TZ environment variable: '{tz_env}'. "
                    f"Falling back to system timezone. "
                    f"Use a valid IANA timezone name (e.g., 'Europe/Copenhagen').",
                    UserWarning,
                    stacklevel=2,
                )

        if local_tz is None:
            local_tz = datetime.now().astimezone().tzinfo

        # Localize to local timezone - this applies DST rules based on the date
        if local_tz is not None:
            try:
                # For pytz timezones, use localize method with is_dst=True to prefer first occurrence
                if hasattr(local_tz, "localize"):
                    # is_dst=True means during ambiguous times (fall-back), use the first occurrence (DST active)
                    # pytz timezones provide 'localize', but type checker does not recognize it; checked with hasattr above
                    aware_dt = local_tz.localize(naive_dt, is_dst=True)  # type: ignore[attr-defined]
                else:
                    # For other timezone implementations (e.g., zoneinfo)
                    aware_dt = naive_dt.replace(tzinfo=local_tz)

                # Convert to UTC for internal consistency, then remove timezone
                # Database expects naive local time, but we work in UTC internally
                utc_dt = aware_dt.astimezone(timezone.utc)
                return utc_dt.replace(tzinfo=None)
            except Exception:
                # Fallback: return naive datetime (treat as if already UTC)
                return naive_dt

        return naive_dt

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

        batch_timestamp = reference_time.strftime("%Y%m%d_%H%M%S")

        # Priority: --start + duration, then duration alone, then --start + --end
        if hasattr(args, "start") and args.start and DateRangeCalculator._has_duration(args):
            # --start with duration: start at given time and go forward
            start_dt = DateRangeCalculator._parse_local_datetime(args.start)
            duration = DateRangeCalculator._extract_duration(args)
            end_dt = start_dt + duration

            # Use start time for batch timestamp (consistent filenames)
            batch_timestamp = start_dt.strftime("%Y%m%d_%H%M%S")

            return DateRange(
                start=start_dt, end=end_dt, batch_timestamp=batch_timestamp, is_relative=False
            )

        if DateRangeCalculator._has_duration(args):
            # Duration alone: go back N minutes/hours/days from now
            duration = DateRangeCalculator._extract_duration(args)
            end_dt = reference_time
            start_dt = end_dt - duration

            return DateRange(
                start=start_dt, end=end_dt, batch_timestamp=batch_timestamp, is_relative=True
            )

        if hasattr(args, "start") and hasattr(args, "end") and args.start and args.end:
            # Absolute time range
            start_dt = DateRangeCalculator._parse_local_datetime(args.start)
            end_dt = DateRangeCalculator._parse_local_datetime(args.end)

            return DateRange(
                start=start_dt,
                end=end_dt,
                batch_timestamp=None,  # No batch timestamp for absolute ranges
                is_relative=False,
            )

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
        batch_timestamp = reference_time.strftime("%Y%m%d_%H%M%S")

        return DateRange(
            start=start_dt, end=end_dt, batch_timestamp=batch_timestamp, is_relative=True
        )

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
        batch_timestamp = start_dt.strftime("%Y%m%d_%H%M%S")

        return DateRange(
            start=start_dt, end=end_dt, batch_timestamp=batch_timestamp, is_relative=False
        )

    @staticmethod
    def _has_duration(args) -> bool:
        """Check if args contain any duration specification."""
        return (
            (hasattr(args, "minutes") and args.minutes)
            or (hasattr(args, "hours") and args.hours)
            or (hasattr(args, "days") and args.days)
        )

    @staticmethod
    def _extract_duration(args) -> timedelta:
        """Extract timedelta from args duration fields."""
        if hasattr(args, "minutes") and args.minutes:
            return timedelta(minutes=args.minutes)
        if hasattr(args, "hours") and args.hours:
            return timedelta(hours=args.hours)
        if hasattr(args, "days") and args.days:
            return timedelta(days=args.days)
        raise ValueError("No duration specified in args")
