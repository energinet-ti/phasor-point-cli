"""
Extraction history manager for storing and retrieving historical extraction performance data.

Tracks server-wide extraction performance to provide time estimates for future extractions.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class ExtractionMetrics:
    """Performance metrics from a completed extraction."""

    timestamp: str
    rows: int
    duration_sec: float
    chunk_size_minutes: int
    parallel_workers: int
    rows_per_sec: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "rows": self.rows,
            "duration_sec": self.duration_sec,
            "chunk_size_minutes": self.chunk_size_minutes,
            "parallel_workers": self.parallel_workers,
            "rows_per_sec": self.rows_per_sec,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExtractionMetrics:
        return cls(
            timestamp=data["timestamp"],
            rows=data["rows"],
            duration_sec=data["duration_sec"],
            chunk_size_minutes=data["chunk_size_minutes"],
            parallel_workers=data["parallel_workers"],
            rows_per_sec=data["rows_per_sec"],
        )


class ExtractionHistory:
    """Manages historical extraction performance data for time estimation."""

    MAX_HISTORY_SIZE = 50

    def __init__(self, config_path_manager, logger=None):
        """
        Initialize extraction history manager.

        Args:
            config_path_manager: ConfigPathManager instance to determine storage location
            logger: Optional logger instance
        """
        self.config_path_manager = config_path_manager
        self.logger = logger
        self._history_file: Path | None = None
        self._extractions: list[ExtractionMetrics] = []
        self._save_counter = 0
        self._save_frequency = 1  # Save every N additions to reduce disk I/O

    def _get_history_file_path(self) -> Path:
        """Get the path to the extraction history file."""
        if self._history_file is not None:
            return self._history_file

        # Check if local config exists, if so use local directory
        local_config = self.config_path_manager.get_local_config_file()
        if local_config.exists():
            history_file = local_config.parent / "extraction_history.json"
        else:
            # Use user config directory
            config_dir = self.config_path_manager.get_user_config_dir()
            history_file = config_dir / "extraction_history.json"

        self._history_file = history_file
        return history_file

    def load_history(self) -> None:
        """Load extraction history from disk."""
        history_file = self._get_history_file_path()

        if not history_file.exists():
            if self.logger:
                self.logger.debug(f"No extraction history file found at {history_file}")
            self._extractions = []
            return

        try:
            with history_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
                extractions_data = data.get("extractions", [])
                self._extractions = [ExtractionMetrics.from_dict(item) for item in extractions_data]
            if self.logger:
                self.logger.debug(
                    f"Loaded {len(self._extractions)} extraction records from history"
                )
        except Exception as exc:
            if self.logger:
                self.logger.warning(f"Failed to load extraction history: {exc}")
            self._extractions = []

    def save_history(self) -> None:
        """Save extraction history to disk."""
        history_file = self._get_history_file_path()

        # Ensure directory exists
        history_file.parent.mkdir(parents=True, exist_ok=True)

        # Prune old entries if needed
        if len(self._extractions) > self.MAX_HISTORY_SIZE:
            self._extractions = self._extractions[-self.MAX_HISTORY_SIZE :]

        try:
            data = {"extractions": [item.to_dict() for item in self._extractions]}
            with history_file.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            if self.logger:
                self.logger.debug(f"Saved {len(self._extractions)} extraction records to history")
        except Exception as exc:
            if self.logger:
                self.logger.warning(f"Failed to save extraction history: {exc}")

    def add_extraction(
        self,
        rows: int,
        duration_sec: float,
        chunk_size_minutes: int,
        parallel_workers: int,
    ) -> None:
        """
        Add a new extraction record to history.

        Args:
            rows: Number of rows extracted
            duration_sec: Duration in seconds
            chunk_size_minutes: Chunk size used
            parallel_workers: Number of parallel workers
        """
        if duration_sec <= 0:
            if self.logger:
                self.logger.warning("Cannot add extraction with zero or negative duration")
            return

        rows_per_sec = rows / duration_sec

        metrics = ExtractionMetrics(
            timestamp=datetime.now().isoformat(),
            rows=rows,
            duration_sec=duration_sec,
            chunk_size_minutes=chunk_size_minutes,
            parallel_workers=parallel_workers,
            rows_per_sec=rows_per_sec,
        )

        self._extractions.append(metrics)
        self._save_counter += 1

        # Save every N additions to reduce disk I/O
        if self._save_counter >= self._save_frequency:
            self.save_history()
            self._save_counter = 0

    def get_average_rows_per_sec(self, recent_n: int = 10) -> float | None:
        """
        Get average rows per second from recent extractions.

        Args:
            recent_n: Number of recent extractions to consider

        Returns:
            Average rows per second, or None if no history
        """
        if not self._extractions:
            return None

        recent = self._extractions[-recent_n:]
        if not recent:
            return None

        total_rows_per_sec = sum(e.rows_per_sec for e in recent)
        return total_rows_per_sec / len(recent)

    def estimate_duration(self, estimated_rows: int) -> float | None:
        """
        Estimate extraction duration based on historical performance.

        Args:
            estimated_rows: Expected number of rows to extract

        Returns:
            Estimated duration in seconds, or None if no history
        """
        avg_rate = self.get_average_rows_per_sec()
        if avg_rate is None or avg_rate <= 0:
            return None

        return estimated_rows / avg_rate

    def get_history_count(self) -> int:
        """Get the number of extraction records in history."""
        return len(self._extractions)

    def flush(self) -> None:
        """Force save any pending changes to disk."""
        if self._save_counter > 0:
            self.save_history()
            self._save_counter = 0
