"""
Core data models used by the refactored PhasorPoint CLI.

The dataclasses defined in this module provide well structured containers for
domain entities (PMU metadata, extraction requests/results, validation
outcomes, etc.) that previously relied on loosely typed dictionaries.

Each dataclass includes minimal helper behaviour such as simple validation or
serialisation helpers where that keeps consumers concise. The intent is to keep
models lightweight and focussed on data, leaving richer logic to the
corresponding service/manager classes introduced during the migration.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


def _serialise_optional_datetime(value: datetime | None) -> str | None:
    """Serialise datetimes using ISO format for deterministic comparisons."""
    return value.isoformat() if isinstance(value, datetime) else None


@dataclass(slots=True)
class PMUInfo:
    """Metadata describing an individual PMU."""

    id: int
    station_name: str
    country: str = ""
    extra_attributes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialise the PMU info into a JSON friendly dict."""
        payload = asdict(self)
        payload["extra_attributes"] = dict(self.extra_attributes)
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PMUInfo:
        """Create an instance from a configuration style dictionary."""
        return cls(
            id=int(data["id"]),
            station_name=data.get("station_name", ""),
            country=data.get("country", ""),
            extra_attributes={
                key: value
                for key, value in data.items()
                if key not in {"id", "station_name", "country"}
            },
        )


@dataclass(slots=True)
class DataQualityThresholds:
    """Threshold configuration used by validation logic."""

    frequency_min: float
    frequency_max: float
    null_threshold_percent: float
    gap_multiplier: float

    def validate(self) -> None:
        """Validate that thresholds are internally consistent."""
        if self.frequency_min >= self.frequency_max:
            raise ValueError("frequency_min must be less than frequency_max")
        if not 0 <= self.null_threshold_percent <= 100:
            raise ValueError("null_threshold_percent must be within 0-100")
        if self.gap_multiplier <= 0:
            raise ValueError("gap_multiplier must be positive")

    def to_dict(self) -> dict[str, float]:
        """Return a primitive dictionary representation."""
        return {
            "frequency_min": float(self.frequency_min),
            "frequency_max": float(self.frequency_max),
            "null_threshold_percent": float(self.null_threshold_percent),
            "gap_multiplier": float(self.gap_multiplier),
        }


@dataclass(slots=True)
class TableStatistics:
    """Summary statistics captured while inspecting a PMU table."""

    row_count: int
    column_count: int
    start_time: datetime | None = None
    end_time: datetime | None = None
    bytes_estimate: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialise statistics for logging or JSON output."""
        return {
            "row_count": int(self.row_count),
            "column_count": int(self.column_count),
            "start_time": _serialise_optional_datetime(self.start_time),
            "end_time": _serialise_optional_datetime(self.end_time),
            "bytes_estimate": self.bytes_estimate,
        }

    @property
    def duration(self) -> timedelta | None:
        """Return duration covered by table rows if timestamps available."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


@dataclass(slots=True)
class TableDiscoveryResult:
    """Outcome from attempting to locate a PMU table."""

    table_name: str
    pmu_id: int
    resolution: int
    found: bool
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "pmu_id": int(self.pmu_id),
            "resolution": int(self.resolution),
            "found": bool(self.found),
            "error": self.error,
        }


@dataclass(slots=True)
class TableInfo:
    """Combined information about a PMU table after discovery."""

    pmu_id: int
    resolution: int
    table_name: str
    statistics: TableStatistics | None = None
    pmu_info: PMUInfo | None = None
    sample_data: pd.DataFrame | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialise to primitive types, DataFrame converted to records."""
        payload: dict[str, Any] = {
            "pmu_id": int(self.pmu_id),
            "resolution": int(self.resolution),
            "table_name": self.table_name,
            "statistics": self.statistics.to_dict() if self.statistics else None,
            "pmu_info": self.pmu_info.to_dict() if self.pmu_info else None,
        }
        if self.sample_data is not None:
            payload["sample_data"] = self.sample_data.to_dict(orient="records")
        else:
            payload["sample_data"] = None
        return payload


@dataclass(slots=True)
class TableListResult:
    """Result of listing available PMU tables."""

    found_pmus: dict[int, list[int]]  # PMU ID -> list of resolutions

    @property
    def total_tables(self) -> int:
        """Total number of accessible tables found."""
        return sum(len(resolutions) for resolutions in self.found_pmus.values())

    def to_dict(self) -> dict[str, Any]:
        """Serialise to primitive types."""
        return {
            "found_pmus": {
                int(pmu_id): list(resolutions) for pmu_id, resolutions in self.found_pmus.items()
            },
            "total_tables": int(self.total_tables),
        }


@dataclass
class DateRange:
    """
    Date range with half-open interval [start, end) used within extraction flows.

    Stores user-provided local times and provides conversion methods
    for different use cases (database queries, filenames, logging).

    The interval is half-open: start is inclusive, end is exclusive.
    This prevents duplicate timestamps when concatenating sequential exports.

    Note: Conversion methods import DateRangeCalculator inline to avoid
    circular imports (date_utils imports DateRange).
    """

    start: datetime  # User's local time as provided (inclusive)
    end: datetime  # User's local time as provided (exclusive)

    def validate(self) -> None:
        if self.start > self.end:
            raise ValueError("start must be before or equal to end")

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    def to_strings(self, fmt: str = "%Y-%m-%d %H:%M:%S") -> dict[str, str]:
        """Return formatted start/end strings for SQL or logging."""
        return {"start": self.start.strftime(fmt), "end": self.end.strftime(fmt)}

    def as_database_time(self) -> tuple[datetime, datetime]:
        """
        Convert user's local time to database time (CET, UTC+1 fixed, no DST).

        Returns:
            Tuple of (start, end) in database timezone for SQL queries
        """
        from .date_utils import DateRangeCalculator  # noqa: PLC0415 - avoid circular import

        db_start = DateRangeCalculator.convert_to_database_time(self.start)
        db_end = DateRangeCalculator.convert_to_database_time(self.end)
        return db_start, db_end

    def as_filename_format(self) -> tuple[str, str]:
        """
        Format start/end for filename generation.

        Returns:
            Tuple of (start_str, end_str) in YYYYMMDD_HHMMSS format
        """
        return (
            self.start.strftime("%Y%m%d_%H%M%S"),
            self.end.strftime("%Y%m%d_%H%M%S"),
        )

    def as_utc_offset_strings(self) -> tuple[str, str]:
        """
        Get UTC offset strings for start/end times.

        Returns:
            Tuple of (start_offset, end_offset) in format "+HH:MM" or "-HH:MM"
        """
        from .date_utils import DateRangeCalculator  # noqa: PLC0415 - avoid circular import

        start_offset = DateRangeCalculator.get_utc_offset(self.start)
        end_offset = DateRangeCalculator.get_utc_offset(self.end)
        return start_offset, end_offset

    def get_timezone_name(self) -> str:
        """
        Get the timezone name for the user's local timezone.

        Returns:
            Timezone name string (e.g., "Europe/Copenhagen", "UTC")
        """
        from .date_utils import DateRangeCalculator  # noqa: PLC0415 - avoid circular import

        local_tz = DateRangeCalculator.get_local_timezone()
        return str(local_tz) if local_tz else "UTC"


@dataclass(slots=True)
class ExtractionRequest:
    """Parameters describing a single extraction run."""

    pmu_id: int
    date_range: DateRange
    output_file: Path | None = None
    resolution: int = 1
    processed: bool = True
    clean: bool = True
    chunk_size_minutes: int = 15
    parallel_workers: int = 1
    output_format: str = "parquet"
    replace: bool = False

    def validate(self) -> None:
        """Ensure the request parameters are coherent."""
        self.date_range.validate()
        if self.resolution <= 0:
            raise ValueError("resolution must be positive")
        if self.chunk_size_minutes <= 0:
            raise ValueError("chunk_size_minutes must be positive")
        if self.parallel_workers <= 0:
            raise ValueError("parallel_workers must be positive")
        if self.output_format not in {"parquet", "csv"}:
            raise ValueError("output_format must be 'parquet' or 'csv'")

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "pmu_id": int(self.pmu_id),
            "resolution": int(self.resolution),
            "processed": bool(self.processed),
            "clean": bool(self.clean),
            "chunk_size_minutes": int(self.chunk_size_minutes),
            "parallel_workers": int(self.parallel_workers),
            "output_format": self.output_format,
            "date_range": self.date_range.to_strings(),
            "replace": bool(self.replace),
        }
        if self.output_file is not None:
            payload["output_file"] = str(self.output_file)
        else:
            payload["output_file"] = None
        return payload


@dataclass(slots=True)
class ChunkResult:
    """Record of a single chunk extraction attempt."""

    chunk_index: int
    start: datetime
    end: datetime
    rows: int
    duration_seconds: float
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "chunk_index": int(self.chunk_index),
            "start": _serialise_optional_datetime(self.start),
            "end": _serialise_optional_datetime(self.end),
            "rows": int(self.rows),
            "duration_seconds": float(self.duration_seconds),
            "error": self.error,
        }


@dataclass(slots=True)
class PersistResult:
    """Result of persisting a DataFrame to disk."""

    output_path: Path
    file_size_mb: float
    skip_result: ExtractionResult | None = None


@dataclass(slots=True)
class ExtractionResult:
    """Outcome of attempting a single extraction."""

    request: ExtractionRequest
    success: bool
    output_file: Path | None
    rows_extracted: int
    extraction_time_seconds: float
    file_size_mb: float | None = None
    error: str | None = None
    chunk_results: Sequence[ChunkResult] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": bool(self.success),
            "output_file": str(self.output_file) if self.output_file else None,
            "rows_extracted": int(self.rows_extracted),
            "extraction_time_seconds": float(self.extraction_time_seconds),
            "file_size_mb": float(self.file_size_mb) if self.file_size_mb is not None else None,
            "error": self.error,
            "request": self.request.to_dict(),
            "chunks": [chunk.to_dict() for chunk in self.chunk_results],
        }

    def has_errors(self) -> bool:
        return not self.success or any(chunk.error for chunk in self.chunk_results)


@dataclass(slots=True)
class BatchExtractionResult:
    """Aggregated outcome for a batch extraction session."""

    batch_id: str
    results: Sequence[ExtractionResult] = field(default_factory=tuple)
    started_at: datetime | None = None
    finished_at: datetime | None = None

    def successful_results(self) -> list[ExtractionResult]:
        return [result for result in self.results if result.success]

    def failed_results(self) -> list[ExtractionResult]:
        return [result for result in self.results if not result.success]

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "started_at": _serialise_optional_datetime(self.started_at),
            "finished_at": _serialise_optional_datetime(self.finished_at),
            "results": [result.to_dict() for result in self.results],
        }


@dataclass(slots=True)
class ValidationCheck:
    """Represents an individual validation rule result."""

    name: str
    passed: bool
    details: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "passed": bool(self.passed), "details": self.details}


@dataclass(slots=True)
class ValidationResult:
    """Aggregate validation outcome for a dataset."""

    checks: Sequence[ValidationCheck]
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @property
    def is_successful(self) -> bool:
        return all(check.passed for check in self.checks)

    def to_dict(self) -> dict[str, Any]:
        return {
            "checks": [check.to_dict() for check in self.checks],
            "started_at": _serialise_optional_datetime(self.started_at),
            "finished_at": _serialise_optional_datetime(self.finished_at),
            "is_successful": self.is_successful,
        }


@dataclass(slots=True)
class PhasorColumnMap:
    """Describes the per-phase column selections used for phasor calculations."""

    voltage_magnitude: dict[str, str] = field(default_factory=dict)
    voltage_angle: dict[str, str] = field(default_factory=dict)
    current_magnitude: dict[str, str] = field(default_factory=dict)
    current_angle: dict[str, str] = field(default_factory=dict)
    frequency: list[str] = field(default_factory=list)
    extra_columns: dict[str, Iterable[str]] = field(default_factory=dict)

    def combined_columns(self) -> list[str]:
        """Return a flattened list of the selected columns."""
        columns: list[str] = []
        columns.extend(self.voltage_magnitude.values())
        columns.extend(self.current_magnitude.values())
        columns.extend(self.voltage_angle.values())
        columns.extend(self.current_angle.values())
        columns.extend(self.frequency)
        for group in self.extra_columns.values():
            columns.extend(group)
        return columns


@dataclass(slots=True)
class QueryResult:
    """Outcome of executing an ad-hoc query command."""

    success: bool
    rows_returned: int
    duration_seconds: float
    output_file: Path | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": bool(self.success),
            "rows_returned": int(self.rows_returned),
            "duration_seconds": float(self.duration_seconds),
            "output_file": str(self.output_file) if self.output_file else None,
            "error": self.error,
        }


@dataclass
class WriteResult:
    """Result of persisting a dataframe to disk."""

    success: bool
    output_file: Path
    file_size_mb: float
    row_count: int
    column_count: int
    format: str
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": bool(self.success),
            "output_file": str(self.output_file),
            "file_size_mb": float(self.file_size_mb),
            "row_count": int(self.row_count),
            "column_count": int(self.column_count),
            "format": self.format,
            "error": self.error,
        }
