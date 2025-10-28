"""
Data processing utilities for PhasorPoint CLI.

``DataProcessor`` orchestrates timestamp handling, type conversion, and (optionally)
data validation via ``DataValidator``.
"""

from __future__ import annotations

import contextlib
import logging
import os
from collections.abc import Iterable, Sequence
from datetime import datetime

import pandas as pd
import pytz
import tzlocal

from .config import ConfigurationManager
from .data_validator import DataValidator
from .models import DataQualityThresholds


class DataProcessor:
    """High level processor responsible for cleaning and validating extracted data."""

    def __init__(
        self,
        config_manager: ConfigurationManager | None = None,
        logger: logging.Logger | None = None,
        validator: DataValidator | None = None,
        output=None,
    ) -> None:
        self.logger = logger or logging.getLogger("phasor_cli")
        self.config_manager = config_manager
        self.output = output

        if validator is not None:
            self.validator = validator
        else:
            thresholds = self._determine_thresholds()
            self.validator = DataValidator(thresholds, logger=self.logger)

    # ---------------------------------------------------------------- Helpers --
    def _determine_thresholds(self) -> DataQualityThresholds:
        if self.config_manager and hasattr(self.config_manager, "get_data_quality_thresholds"):
            return self.config_manager.get_data_quality_thresholds()
        return DataQualityThresholds(
            frequency_min=45,
            frequency_max=65,
            null_threshold_percent=50,
            gap_multiplier=5,
        )

    # ----------------------------------------------------------- Static utils --
    @staticmethod
    def drop_empty_columns(
        df: pd.DataFrame,
        extraction_log: dict | None = None,
        output=None,
    ) -> pd.DataFrame:
        """Drop columns that are completely null/empty."""
        empty_cols = df.columns[df.isnull().all()].tolist()
        if empty_cols:
            if output:
                output.info(f"Dropping {len(empty_cols)} completely empty columns", tag="INFO")
            if extraction_log is not None:
                for column in empty_cols:
                    extraction_log["column_changes"]["removed"].append(
                        {
                            "column": column,
                            "reason": "completely_empty",
                            "description": "All values were null",
                        }
                    )
            df = df.drop(columns=empty_cols)
        return df

    @staticmethod
    def get_local_timezone() -> datetime.tzinfo | None:
        """Detect local timezone, preferring ``TZ`` environment variable."""
        tz_env = os.environ.get("TZ")
        if tz_env:
            try:
                return pytz.timezone(tz_env)
            except Exception:  # pragma: no cover - graceful fallback
                pass
        try:
            return tzlocal.get_localzone()
        except Exception:  # pragma: no cover - graceful fallback
            return pytz.UTC

    @staticmethod
    def format_timestamps_with_precision(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
        for column in columns:
            if column not in df.columns:
                continue
            try:
                series = df[column]
                if len(df) > 0 and hasattr(series.iloc[0], "microsecond"):
                    df[column] = series.apply(
                        lambda value: value.strftime("%Y-%m-%d %H:%M:%S")
                        + f".{value.microsecond // 1000:03d}"
                        if hasattr(value, "microsecond")
                        else str(value)
                    )
                elif series.dtype == "object":
                    parsed = pd.to_datetime(series, errors="coerce")
                    df[column] = parsed.apply(
                        lambda value: value.strftime("%Y-%m-%d %H:%M:%S")
                        + f".{value.microsecond // 1000:03d}"
                        if hasattr(value, "microsecond")
                        else str(value)
                    )
            except Exception:  # pragma: no cover - fallback
                with contextlib.suppress(Exception):
                    df[column] = df[column].astype(str)
        return df

    @staticmethod
    def convert_columns_to_numeric(
        df: pd.DataFrame,
        extraction_log: dict | None = None,
        logger: logging.Logger | None = None,
        output=None,
    ) -> pd.DataFrame:
        non_ts_cols = [column for column in df.columns if column not in ["ts", "ts_local"]]
        converted_count = 0

        for column in non_ts_cols:
            try:
                if df[column].dtype == "object":
                    original_nulls = df[column].isnull().sum()
                    original_type = str(df[column].dtype)
                    df[column] = pd.to_numeric(df[column], errors="coerce")
                    new_nulls = df[column].isnull().sum()
                    new_type = str(df[column].dtype)

                    if extraction_log is not None:
                        extraction_log["column_changes"]["type_conversions"].append(
                            {
                                "column": column,
                                "from_type": original_type,
                                "to_type": new_type,
                                "nulls_before": int(original_nulls),
                                "nulls_after": int(new_nulls),
                            }
                        )

                    if new_nulls > original_nulls:
                        added_nulls = new_nulls - original_nulls
                        if added_nulls > 0:
                            if output:
                                output.warning(
                                    f"{column}: {added_nulls} non-numeric values converted to NaN"
                                )
                            if extraction_log is not None:
                                extraction_log["issues_found"].append(
                                    {
                                        "type": "non_numeric_values",
                                        "column": column,
                                        "count": int(added_nulls),
                                        "description": f"{added_nulls} non-numeric values converted to NaN",
                                    }
                                )
                    converted_count += 1
            except Exception as exc:  # pragma: no cover - log warning
                if logger:
                    logger.warning(f"Could not convert {column}: {exc}")
                if extraction_log is not None:
                    extraction_log["issues_found"].append(
                        {
                            "type": "conversion_error",
                            "column": column,
                            "error": str(exc),
                        }
                    )

        if logger:
            logger.info(f"Converted {converted_count} columns to numeric types")
        return df

    @classmethod
    def apply_timezone_conversion(
        cls,
        df: pd.DataFrame,
        extraction_log: dict | None = None,
        timezone_factory=None,
        output=None,
    ) -> pd.DataFrame:
        try:
            local_tz = timezone_factory() if timezone_factory else cls.get_local_timezone()
        except Exception as exc:
            df = cls.format_timestamps_with_precision(df, ["ts"])
            if extraction_log is not None:
                extraction_log["issues_found"].append(
                    {
                        "type": "timestamp_adjustment_error",
                        "error": str(exc),
                    }
                )
            return df

        try:
            if local_tz is not None:
                # Database returns ts in UTC - keep it unchanged
                # Create ts_local with per-row DST-aware conversion
                df["ts_local"] = pd.to_datetime(df["ts"])
                df["ts_local"] = df["ts_local"].dt.tz_localize("UTC")
                df["ts_local"] = df["ts_local"].dt.tz_convert(local_tz)
                df["ts_local"] = df["ts_local"].dt.tz_localize(None)  # Remove tz info

                # Format both timestamp columns with precision
                df = cls.format_timestamps_with_precision(df, ["ts", "ts_local"])

                # Calculate offset from actual data timestamps (not current time)
                first_utc = pd.to_datetime(df["ts"].iloc[0])
                first_local = pd.to_datetime(df["ts_local"].iloc[0])
                last_utc = pd.to_datetime(df["ts"].iloc[-1])
                last_local = pd.to_datetime(df["ts_local"].iloc[-1])

                first_offset = (first_local - first_utc).total_seconds() / 3600
                last_offset = (last_local - last_utc).total_seconds() / 3600

                # Check if data crosses DST boundary
                dst_transition = abs(first_offset - last_offset) > 0.01

                if output:
                    if dst_transition:
                        output.info(
                            f"Created dual timestamp columns:\n"
                            f"  - ts: UTC (authoritative)\n"
                            f"  - ts_local: Local time (UTC{first_offset:+.1f} at start, "
                            f"UTC{last_offset:+.1f} at end - DST transition detected)",
                            tag="TIME",
                        )
                    else:
                        output.info(
                            f"Created dual timestamp columns:\n"
                            f"  - ts: UTC (authoritative)\n"
                            f"  - ts_local: Local time (UTC{first_offset:+.1f} offset)",
                            tag="TIME",
                        )

                if extraction_log is not None:
                    extraction_log["data_quality"]["timestamp_adjustment"] = {
                        "method": "per_row_dst_aware",
                        "offset_hours_start": round(first_offset, 2),
                        "offset_hours_end": round(last_offset, 2),
                        "dst_transition": dst_transition,
                        "timezone": str(local_tz),
                        "description": (
                            "ts column kept as UTC (from database). Created ts_local with per-row "
                            f"DST-aware conversion using timezone {local_tz}"
                        ),
                        "columns_added": ["ts_local"],
                        "columns_modified": [],
                    }
            else:
                if output:
                    output.warning("Could not determine machine timezone, keeping UTC timestamps")
                df = cls.format_timestamps_with_precision(df, ["ts"])
                if extraction_log is not None:
                    extraction_log["issues_found"].append(
                        {
                            "type": "timezone_detection_error",
                            "error": "Could not determine machine timezone",
                        }
                    )
        except Exception as exc:  # pragma: no cover - still format columns
            df = cls.format_timestamps_with_precision(df, ["ts"])
            if extraction_log is not None:
                extraction_log["issues_found"].append(
                    {
                        "type": "timestamp_adjustment_error",
                        "error": str(exc),
                    }
                )
        return df

    # ----------------------------------------------------------- Public Interface ---
    def clean_and_convert_types(
        self, df: pd.DataFrame | None, extraction_log: dict | None = None
    ) -> pd.DataFrame | None:
        if df is None or len(df) == 0:
            if self.logger:
                self.logger.warning("No data to clean")
            return df

        if self.logger:
            self.logger.info("Cleaning and converting data types...")

        # Drop empty columns FIRST, before any type conversions
        df = self.drop_empty_columns(df, extraction_log, self.output)

        if "ts" in df.columns:
            df = self.apply_timezone_conversion(df, extraction_log, output=self.output)

        # Format timestamps - check which columns exist
        ts_cols = [col for col in ["ts", "ts_local"] if col in df.columns]
        df = self.format_timestamps_with_precision(df, ts_cols)
        return self.convert_columns_to_numeric(df, extraction_log, self.logger, self.output)

    def process(
        self,
        df: pd.DataFrame | None,
        extraction_log: dict | None = None,
        *,
        clean: bool = True,
        validate: bool = True,
    ):
        if df is None:
            return None, []

        processed_df = df
        if clean:
            processed_df = self.clean_and_convert_types(processed_df, extraction_log)
            if processed_df is None:
                return None, []

        issues: Iterable[str] = []
        if validate and self.validator and processed_df is not None:
            processed_df, issues = self.validator.validate(processed_df, extraction_log)

        return processed_df, list(issues)
