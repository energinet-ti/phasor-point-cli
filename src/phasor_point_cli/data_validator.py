"""
Data validation utilities as part of the OOP refactor.

The ``DataValidator`` class encapsulates the data-quality checks that were
previously implemented as free functions in ``data_processing``. It operates on
``pandas`` DataFrames and records issues to the provided extraction log where
appropriate.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from .models import DataQualityThresholds

if TYPE_CHECKING:
    from .user_output import UserOutput


class DataValidator:
    """Validate extracted PMU data using configurable thresholds."""

    def __init__(
        self,
        thresholds: DataQualityThresholds | dict | None = None,
        logger=None,
        output: UserOutput | None = None,
    ) -> None:
        if thresholds is None:
            thresholds = DataQualityThresholds(
                frequency_min=45,
                frequency_max=65,
                null_threshold_percent=50,
                gap_multiplier=5,
            )
        elif isinstance(thresholds, dict):
            thresholds = DataQualityThresholds(
                frequency_min=thresholds.get("frequency_min") or thresholds.get("freq_min", 45),
                frequency_max=thresholds.get("frequency_max") or thresholds.get("freq_max", 65),
                null_threshold_percent=thresholds.get("null_threshold_percent")
                or thresholds.get("null_threshold", 50),
                gap_multiplier=thresholds.get("gap_multiplier", 5),
            )

        self.thresholds: DataQualityThresholds = thresholds
        self.logger = logger
        self.output = output

    # --------------------------------------------------------------- Checkers --
    def check_empty_columns(
        self, df: pd.DataFrame, extraction_log: dict | None = None
    ) -> tuple[pd.DataFrame, list[str]]:
        issues: list[str] = []
        try:
            empty_cols = df.columns[df.isnull().all()].tolist()
            if empty_cols:
                issues.append(f"Empty columns: {len(empty_cols)}")
                if self.output:
                    self.output.warning(f"Removed {len(empty_cols)} completely empty columns")
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
        except Exception as exc:  # pragma: no cover - defensive logging
            if self.output:
                self.output.warning(f"Error checking empty columns: {exc}")
            if extraction_log is not None:
                extraction_log["issues_found"].append(
                    {
                        "type": "validation_error",
                        "step": "empty_columns_check",
                        "error": str(exc),
                    }
                )
        return df, issues

    def check_null_percentages(
        self,
        df: pd.DataFrame,
        null_threshold: float | None = None,
        extraction_log: dict | None = None,
    ) -> list[str]:
        issues: list[str] = []
        threshold = (
            null_threshold if null_threshold is not None else self.thresholds.null_threshold_percent
        )
        try:
            if len(df) > 0:
                null_pct = df.isnull().sum() / len(df) * 100
                high_null_cols = null_pct[null_pct > threshold].index.tolist()
                if high_null_cols:
                    issues.append(f"High null columns: {len(high_null_cols)}")
                    if self.output:
                        self.output.warning(
                            f"{len(high_null_cols)} columns have >{threshold}% null values"
                        )
                    if extraction_log is not None:
                        for column in high_null_cols:
                            extraction_log["issues_found"].append(
                                {
                                    "type": "high_null_percentage",
                                    "column": column,
                                    "null_percentage": round(float(null_pct[column]), 2),
                                    "threshold": threshold,
                                    "description": f"{null_pct[column]:.1f}% of values are null",
                                }
                            )
        except Exception as exc:  # pragma: no cover - defensive logging
            if self.output:
                self.output.warning(f"Error checking null percentages: {exc}")
            if extraction_log is not None:
                extraction_log["issues_found"].append(
                    {
                        "type": "validation_error",
                        "step": "null_percentage_check",
                        "error": str(exc),
                    }
                )
        return issues

    def check_time_continuity(
        self,
        df: pd.DataFrame,
        gap_multiplier: float | None = None,
        extraction_log: dict | None = None,
    ) -> list[str]:
        issues: list[str] = []
        multiplier = (
            gap_multiplier if gap_multiplier is not None else self.thresholds.gap_multiplier
        )
        try:
            if "ts" in df.columns and len(df) > 1:
                ts_col = pd.to_datetime(df["ts"], errors="coerce")
                ts_sorted = ts_col.sort_values()
                time_diffs = ts_sorted.diff().dropna()
                if len(time_diffs) > 0:
                    median_diff = time_diffs.median()
                    # Type narrowing: ensure median_diff is a Timedelta, not NaT
                    if (
                        pd.notna(median_diff)
                        and isinstance(median_diff, pd.Timedelta)
                        and median_diff > pd.Timedelta(0)
                    ):
                        large_gaps = time_diffs[time_diffs > median_diff * multiplier]
                        if len(large_gaps) > 0:
                            issues.append(f"Time gaps: {len(large_gaps)}")
                            if self.output:
                                self.output.warning(
                                    f"Found {len(large_gaps)} large time gaps (>{multiplier}x median)"
                                )
                            if extraction_log is not None:
                                extraction_log["data_quality"]["time_gaps"] = {
                                    "count": int(len(large_gaps)),
                                    "median_interval": str(median_diff),
                                    "threshold_multiplier": multiplier,
                                    "description": f"{len(large_gaps)} gaps exceeding {multiplier}x the median interval",
                                }
                                extraction_log["issues_found"].append(
                                    {
                                        "type": "time_gaps",
                                        "count": int(len(large_gaps)),
                                        "description": f"Found {len(large_gaps)} large time gaps in data",
                                    }
                                )
        except Exception as exc:  # pragma: no cover - defensive logging
            if self.output:
                self.output.warning(f"Error checking time continuity: {exc}")
            if extraction_log is not None:
                extraction_log["issues_found"].append(
                    {
                        "type": "validation_error",
                        "step": "time_continuity_check",
                        "error": str(exc),
                    }
                )
        return issues

    def check_frequency_ranges(
        self,
        df: pd.DataFrame,
        freq_min: float | None = None,
        freq_max: float | None = None,
        extraction_log: dict | None = None,
    ) -> list[str]:
        issues: list[str] = []
        minimum = freq_min if freq_min is not None else self.thresholds.frequency_min
        maximum = freq_max if freq_max is not None else self.thresholds.frequency_max
        try:
            freq_cols = [
                column
                for column in df.columns
                if column.startswith("f") and not column.startswith("f_")
            ]
            for column in freq_cols:
                if column in df.columns and pd.api.types.is_numeric_dtype(df[column]):
                    valid_data = df[column].dropna()
                    if len(valid_data) > 0:
                        valid_range = valid_data.between(minimum, maximum)
                        invalid_count = (~valid_range).sum()
                        if invalid_count > 0:
                            issues.append(f"Invalid frequency values: {invalid_count}")
                            if self.output:
                                self.output.warning(
                                    f"{column}: {invalid_count} values outside {minimum}-{maximum} Hz range"
                                )
                            if extraction_log is not None:
                                extraction_log["issues_found"].append(
                                    {
                                        "type": "out_of_range_values",
                                        "column": column,
                                        "count": int(invalid_count),
                                        "valid_range": [minimum, maximum],
                                        "description": f"{invalid_count} frequency values outside expected range",
                                    }
                                )
        except Exception as exc:  # pragma: no cover - defensive logging
            if self.output:
                self.output.warning(f"Error checking frequency ranges: {exc}")
            if extraction_log is not None:
                extraction_log["issues_found"].append(
                    {
                        "type": "validation_error",
                        "step": "frequency_range_check",
                        "error": str(exc),
                    }
                )
        return issues

    # --------------------------------------------------------------- Validate --
    def validate(
        self,
        df: pd.DataFrame,
        extraction_log: dict | None = None,
    ) -> tuple[pd.DataFrame, list[str]]:
        if df is None or len(df) == 0:
            if self.logger:
                self.logger.warning("No data to validate")
            return df, ["No data"]

        if self.logger:
            self.logger.info("Validating data quality...")

        if extraction_log is not None:
            extraction_log["data_quality"]["thresholds"] = {
                "frequency_min": self.thresholds.frequency_min,
                "frequency_max": self.thresholds.frequency_max,
                "null_threshold_percent": self.thresholds.null_threshold_percent,
                "gap_multiplier": self.thresholds.gap_multiplier,
            }

        issues: list[str] = []

        df, empty_issues = self.check_empty_columns(df, extraction_log)
        issues.extend(empty_issues)

        null_issues = self.check_null_percentages(
            df,
            self.thresholds.null_threshold_percent,
            extraction_log,
        )
        issues.extend(null_issues)

        time_issues = self.check_time_continuity(
            df,
            self.thresholds.gap_multiplier,
            extraction_log,
        )
        issues.extend(time_issues)

        freq_issues = self.check_frequency_ranges(
            df,
            self.thresholds.frequency_min,
            self.thresholds.frequency_max,
            extraction_log,
        )
        issues.extend(freq_issues)

        if self.output:
            if not issues:
                self.output.info("Data validation passed - no major issues found", tag="OK")
            else:
                self.output.warning(f"Data validation found {len(issues)} issue types")

        if extraction_log is not None:
            extraction_log["data_quality"]["validation_summary"] = {
                "issues_found": len(issues),
                "passed": len(issues) == 0,
            }

        return df, issues
