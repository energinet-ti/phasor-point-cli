"""
Date utility functions for PhasorPoint CLI.

Provides utilities for calculating date ranges from various input formats including
relative durations and absolute timestamps.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from .models import DateRange


class DateRangeCalculator:
    """Calculates date ranges from command arguments."""

    @staticmethod
    def _parse_local_datetime(date_string: str) -> datetime:
        """
        Parse a date string as naive local time for database queries.

        Database expects queries in LOCAL TIME, not UTC. This function simply
        parses the string without any timezone conversion.

        Args:
            date_string: Date string to parse (e.g., "2024-07-15 10:00:00")

        Returns:
            Timezone-naive datetime (for database queries in local time)
        """
        import logging  # noqa: PLC0415

        logger = logging.getLogger("phasor_cli")

        # Parse as naive datetime - database expects local time
        naive_dt = pd.to_datetime(date_string).to_pydatetime()
        logger.debug(f"[DST DEBUG] Parsed input as local time for query: {naive_dt}")

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
        if getattr(args, "start", None) and DateRangeCalculator._has_duration(args):
            # --start with duration: start at given time and go forward
            import logging  # noqa: PLC0415

            logger = logging.getLogger("phasor_cli")

            start_dt = DateRangeCalculator._parse_local_datetime(args.start)
            duration = DateRangeCalculator._extract_duration(args)
            end_dt = start_dt + duration

            logger.debug("[DST DEBUG] Date range calculation:")
            logger.debug(f"[DST DEBUG]   Start (UTC): {start_dt}")
            logger.debug(f"[DST DEBUG]   Duration: {duration}")
            logger.debug(f"[DST DEBUG]   End (UTC): {end_dt}")

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
