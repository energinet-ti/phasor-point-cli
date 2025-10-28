"""
User-facing output management for PhasorPoint CLI.

Provides clean, structured output to the terminal while keeping technical logging
separate in log files. Supports multiple output formats (human-readable, JSON)
for future extensibility.
"""

import json
import sys
from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class OutputFormatter(ABC):
    """Abstract base class for output formatters."""

    @abstractmethod
    def section_header(self, title: str) -> str:
        """Format a section header."""

    @abstractmethod
    def info(self, message: str, tag: str | None = None) -> str:
        """Format an info message with optional tag."""

    @abstractmethod
    def warning(self, message: str) -> str:
        """Format a warning message."""

    @abstractmethod
    def data_summary(self, df: pd.DataFrame, title: str | None = None) -> str:
        """Format a data summary from a DataFrame."""

    @abstractmethod
    def batch_progress(self, completed: int, total: int, pmu_id: int) -> str:
        """Format batch progress message."""

    @abstractmethod
    def skip_message(self, filepath: str, reason: str) -> str:
        """Format a skip notification message."""

    @abstractmethod
    def batch_summary(
        self, total: int, successful: int, failed: int, skipped: int, time_elapsed: float
    ) -> str:
        """Format a batch summary."""


class HumanFormatter(OutputFormatter):
    """Human-readable terminal output formatter."""

    def section_header(self, title: str) -> str:
        """Format a section header with separator lines."""
        separator = "=" * 70
        return f"\n{separator}\n{title}\n{separator}"

    def info(self, message: str, tag: str | None = None) -> str:
        """Format an info message with optional tag."""
        if tag:
            return f"[{tag}] {message}"
        return message

    def warning(self, message: str) -> str:
        """Format a warning message."""
        return f"[WARNING] {message}"

    def data_summary(self, df: pd.DataFrame, title: str | None = None) -> str:
        """Format a data summary from a DataFrame."""
        lines = []

        if title:
            lines.append(self.section_header(title))

        lines.append(f"   Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

        # Time range if 'ts' column exists
        if "ts" in df.columns:
            try:
                ts_col = pd.to_datetime(df["ts"], errors="coerce")
                start_time = ts_col.min()
                end_time = ts_col.max()
                if pd.notna(start_time) and pd.notna(end_time):
                    lines.append(f"   Time range: {start_time} to {end_time}")
            except Exception:
                # Silently skip time range if timestamp parsing fails
                pass

        # Column info
        if df.shape[1] <= 30:
            lines.append(f"   Columns: {', '.join(df.columns.tolist())}")
        else:
            sample_cols = df.columns.tolist()[:10]
            lines.append(f"   Columns ({df.shape[1]} total): {', '.join(sample_cols)}...")

        # Data types summary
        dtype_counts = df.dtypes.value_counts()
        dtype_summary = ", ".join([f"{count} {dtype}" for dtype, count in dtype_counts.items()])
        lines.append(f"   Types: {dtype_summary}")

        # Memory usage
        memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        lines.append(f"   Memory: {memory_mb:.2f} MB")

        return "\n".join(lines)

    def batch_progress(self, completed: int, total: int, pmu_id: int) -> str:
        """Format batch progress message."""
        return f"[BATCH] Processing PMU {pmu_id} ({completed}/{total})"

    def skip_message(self, filepath: str, reason: str) -> str:
        """Format a skip notification message."""
        return f"[SKIP] {filepath} - {reason}"

    def batch_summary(
        self, total: int, successful: int, failed: int, skipped: int, time_elapsed: float
    ) -> str:
        """Format a batch summary."""
        lines = []
        lines.append(self.section_header("Batch Extraction Complete"))
        lines.append(f"   Total PMUs: {total}")
        lines.append(f"   Successful: {successful}")
        if failed > 0:
            lines.append(f"   Failed: {failed}")
        if skipped > 0:
            lines.append(f"   Skipped: {skipped}")
        lines.append(f"   Time elapsed: {time_elapsed:.2f}s")
        lines.append("=" * 70)
        return "\n".join(lines)


class JsonFormatter(OutputFormatter):
    """JSON Lines output formatter for machine-readable output."""

    def _to_json(self, data: dict[str, Any]) -> str:
        """Convert dict to JSON string."""
        return json.dumps(data, default=str)

    def section_header(self, title: str) -> str:
        """Format a section header as JSON."""
        return self._to_json({"type": "section_header", "title": title})

    def info(self, message: str, tag: str | None = None) -> str:
        """Format an info message as JSON."""
        data: dict[str, Any] = {"type": "info", "message": message}
        if tag:
            data["tag"] = tag
        return self._to_json(data)

    def warning(self, message: str) -> str:
        """Format a warning message as JSON."""
        return self._to_json({"type": "warning", "message": message})

    def data_summary(self, df: pd.DataFrame, title: str | None = None) -> str:
        """Format a data summary as JSON."""
        data: dict[str, Any] = {
            "type": "data_summary",
            "shape": list(df.shape),
            "columns": df.columns.tolist(),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "memory_bytes": int(df.memory_usage(deep=True).sum()),
        }

        if title:
            data["title"] = title

        # Add time range if ts column exists
        if "ts" in df.columns:
            try:
                ts_col = pd.to_datetime(df["ts"], errors="coerce")
                start_time = ts_col.min()
                end_time = ts_col.max()
                if pd.notna(start_time) and pd.notna(end_time):
                    data["time_range"] = {
                        "start": str(start_time),
                        "end": str(end_time),
                    }
            except Exception:
                # Silently skip time range if timestamp parsing fails
                pass

        return self._to_json(data)

    def batch_progress(self, completed: int, total: int, pmu_id: int) -> str:
        """Format batch progress as JSON."""
        return self._to_json(
            {
                "type": "batch_progress",
                "completed": completed,
                "total": total,
                "pmu_id": pmu_id,
            }
        )

    def skip_message(self, filepath: str, reason: str) -> str:
        """Format a skip notification as JSON."""
        return self._to_json(
            {
                "type": "skip",
                "filepath": filepath,
                "reason": reason,
            }
        )

    def batch_summary(
        self, total: int, successful: int, failed: int, skipped: int, time_elapsed: float
    ) -> str:
        """Format a batch summary as JSON."""
        return self._to_json(
            {
                "type": "batch_summary",
                "total": total,
                "successful": successful,
                "failed": failed,
                "skipped": skipped,
                "time_elapsed": time_elapsed,
            }
        )


class UserOutput:
    """
    User-facing output orchestrator.

    Manages clean terminal output separate from technical logging.
    Delegates formatting to the provided formatter (HumanFormatter by default).
    """

    def __init__(self, formatter: OutputFormatter | None = None, quiet: bool = False):
        """
        Initialize UserOutput.

        Args:
            formatter: Output formatter to use (defaults to HumanFormatter)
            quiet: If True, suppress all output
        """
        self.formatter = formatter or HumanFormatter()
        self.quiet = quiet
        self.is_tty = sys.stdout.isatty()

    def _print(self, message: str) -> None:
        """Internal print method that respects quiet mode."""
        if not self.quiet:
            print(message)

    def section_header(self, title: str) -> None:
        """Print a section header."""
        self._print(self.formatter.section_header(title))

    def info(self, message: str, tag: str | None = None) -> None:
        """Print an info message with optional tag."""
        self._print(self.formatter.info(message, tag))

    def warning(self, message: str) -> None:
        """Print a warning message."""
        self._print(self.formatter.warning(message))

    def data_summary(self, df: pd.DataFrame, title: str | None = None) -> None:
        """Print a data summary from a DataFrame."""
        self._print(self.formatter.data_summary(df, title))

    def batch_progress(self, completed: int, total: int, pmu_id: int) -> None:
        """Print batch progress message."""
        self._print(self.formatter.batch_progress(completed, total, pmu_id))

    def skip_message(self, filepath: str, reason: str) -> None:
        """Print a skip notification message."""
        self._print(self.formatter.skip_message(filepath, reason))

    def batch_summary(
        self, total: int, successful: int, failed: int, skipped: int, time_elapsed: float
    ) -> None:
        """Print a batch summary."""
        self._print(self.formatter.batch_summary(total, successful, failed, skipped, time_elapsed))

    def blank_line(self) -> None:
        """Print a blank line for spacing."""
        if not self.quiet:
            print()
