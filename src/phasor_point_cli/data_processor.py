"""
Data processing utilities for the OOP refactor.

``DataProcessor`` orchestrates timestamp handling, type conversion, and (optionally)
data validation via ``DataValidator``. Module-level wrappers in
``data_processing`` maintain backwards compatibility with the previous functional
API while delegating to this class.
"""

from __future__ import annotations

import contextlib
import logging
import os
from datetime import datetime, timezone
from typing import Iterable, Sequence

import pandas as pd
import pytz

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
    ) -> None:
        self.logger = logger or logging.getLogger("phasor_cli")
        self.config_manager = config_manager

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
    def get_local_timezone() -> datetime.tzinfo | None:
        """Detect local timezone, preferring ``TZ`` environment variable."""
        tz_env = os.environ.get("TZ")
        if tz_env:
            try:
                return pytz.timezone(tz_env)
            except Exception:  # pragma: no cover - graceful fallback
                pass
        return datetime.now().astimezone().tzinfo

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
    ) -> pd.DataFrame:
        non_ts_cols = [column for column in df.columns if column not in ["ts", "ts_utc"]]
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
                            print(
                                f"   [WARNING]  {column}: {added_nulls} non-numeric values converted to NaN"
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
    ) -> pd.DataFrame:
        try:
            local_tz = timezone_factory() if timezone_factory else cls.get_local_timezone()
        except Exception as exc:
            df = cls.format_timestamps_with_precision(df, ["ts", "ts_utc"])
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
                if df["ts"].dt.tz is None:
                    df["ts"] = df["ts"].dt.tz_localize("UTC")

                df["ts_utc"] = df["ts"].copy()
                df["ts"] = df["ts"].dt.tz_convert(local_tz)
                df["ts"] = df["ts"].dt.tz_localize(None)

                df = cls.format_timestamps_with_precision(df, ["ts", "ts_utc"])

                utc_time = datetime.now(timezone.utc)
                local_time = datetime.now()
                offset_hours = (local_time - utc_time.replace(tzinfo=None)).total_seconds() / 3600
                print(
                    "[TIME] Created dual timestamp columns: ts_utc (original UTC) and ts "
                    f"(local time, {offset_hours:+.1f} hour(s) offset from machine timezone) - "
                    "both formatted consistently with millisecond precision"
                )
                if extraction_log is not None:
                    extraction_log["data_quality"]["timestamp_adjustment"] = {
                        "method": "machine_timezone",
                        "offset_hours": round(offset_hours, 2),
                        "timezone": str(local_tz),
                        "description": (
                            "Created ts_utc (original UTC) and converted ts to local time using "
                            f"machine timezone ({local_tz}) - both formatted consistently with "
                            "millisecond precision"
                        ),
                        "columns_added": ["ts_utc"],
                        "columns_modified": ["ts"],
                    }
            else:
                print("[WARNING]  Could not determine machine timezone, keeping UTC timestamps")
                df = cls.format_timestamps_with_precision(df, ["ts", "ts_utc"])
                if extraction_log is not None:
                    extraction_log["issues_found"].append(
                        {
                            "type": "timezone_detection_error",
                            "error": "Could not determine machine timezone",
                        }
                    )
        except Exception as exc:  # pragma: no cover - still format columns
            df = cls.format_timestamps_with_precision(df, ["ts", "ts_utc"])
            if extraction_log is not None:
                extraction_log["issues_found"].append(
                    {
                        "type": "timestamp_adjustment_error",
                        "error": str(exc),
                    }
                )
        return df

    # ----------------------------------------------------------- Public API ---
    def clean_and_convert_types(
        self, df: pd.DataFrame | None, extraction_log: dict | None = None
    ) -> pd.DataFrame | None:
        if df is None or len(df) == 0:
            if self.logger:
                self.logger.warning("No data to clean")
            return df

        if self.logger:
            self.logger.info("Cleaning and converting data types...")

        if "ts" in df.columns:
            df = self.apply_timezone_conversion(df, extraction_log)

        df = self.format_timestamps_with_precision(df, ["ts", "ts_utc"])
        return self.convert_columns_to_numeric(df, extraction_log, self.logger)

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

        issues: Iterable[str] = []
        if validate and self.validator:
            processed_df, issues = self.validator.validate(processed_df, extraction_log)

        return processed_df, list(issues)
