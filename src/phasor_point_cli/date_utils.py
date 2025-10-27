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
        Parse a date string and convert from system local time to database time (CET).

        The database server expects queries in CET (Central European Time, UTC+1 always, NO DST).
        However, user input is in their system's local time (e.g., CEST = UTC+2 during summer).

        This function converts: System Local Time -> UTC -> CET (for database query)

        Args:
            date_string: Date string to parse (e.g., "2024-07-15 10:00:00")

        Returns:
            Timezone-naive datetime in CET (for database queries)
        """
        import logging  # noqa: PLC0415

        logger = logging.getLogger("phasor_cli")

        # Parse as naive datetime in system local time
        naive_dt = pd.to_datetime(date_string).to_pydatetime()
        logger.debug(f"[DST DEBUG] Parsed input: '{date_string}' -> naive: {naive_dt}")

        # Get system's local timezone
        local_tz = None
        tz_env = os.environ.get("TZ")
        if tz_env:
            try:
                local_tz = pytz.timezone(tz_env)
                logger.debug(f"[DST DEBUG] Using TZ env var: {tz_env}")
            except pytz.exceptions.UnknownTimeZoneError:
                warnings.warn(
                    f"Invalid timezone in TZ environment variable: '{tz_env}'. "
                    f"Falling back to system timezone.",
                    UserWarning,
                    stacklevel=2,
                )

        if local_tz is None:
            try:
                detected_tz = tzlocal.get_localzone()
                tz_name = str(detected_tz)
                if tz_name and not tz_name.startswith("UTC"):
                    try:
                        local_tz = pytz.timezone(tz_name)
                        logger.debug(f"[DST DEBUG] Detected system timezone: {local_tz}")
                    except Exception:
                        local_tz = detected_tz
                else:
                    local_tz = detected_tz
            except Exception as e:
                logger.debug(f"[DST DEBUG] Failed to detect timezone: {e}")
                local_tz = pytz.UTC

        # Localize to system's local timezone (respects DST)
        if local_tz and hasattr(local_tz, "localize"):
            aware_local = local_tz.localize(naive_dt, is_dst=True)  # type: ignore[attr-defined]
            logger.debug(
                f"[DST DEBUG] System local time: {aware_local} ({aware_local.strftime('%Z %z')})"
            )
        else:
            aware_local = naive_dt.replace(tzinfo=local_tz if local_tz else pytz.UTC)
            logger.debug(f"[DST DEBUG] System local time: {aware_local}")

        # Convert to UTC
        utc_dt = aware_local.astimezone(pytz.UTC)
        logger.debug(f"[DST DEBUG] UTC time: {utc_dt}")

        # Convert to database timezone (UTC+1 fixed, no DST)
        # Database uses CET without DST transitions
        from datetime import timedelta  # noqa: PLC0415

        db_offset = timedelta(hours=1)
        db_dt = utc_dt + db_offset
        logger.debug(f"[DST DEBUG] Database time (UTC+1 fixed): {db_dt}")

        # Return as naive datetime for query
        result = db_dt.replace(tzinfo=None)
        logger.debug(f"[DST DEBUG] Final query time: {result}")

        return result

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
        if getattr(args, "start", None) and DateRangeCalculator._has_duration(args):
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

        if getattr(args, "start", None) and getattr(args, "end", None):
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
