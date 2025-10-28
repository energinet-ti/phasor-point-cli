"""
Progress tracker for displaying extraction progress with time estimates.

Provides real-time progress updates with ETA calculations based on historical
and current extraction performance.
"""

from __future__ import annotations

import sys
import threading
import time
from typing import TYPE_CHECKING

from .spinner import Spinner

if TYPE_CHECKING:
    from .extraction_history import ExtractionHistory


class ProgressTracker:
    """Track and display extraction progress with time estimates."""

    def __init__(
        self,
        extraction_history: ExtractionHistory | None = None,
        verbose_timing: bool = False,
        logger=None,
        output=None,
    ):
        """
        Initialize progress tracker.

        Args:
            extraction_history: Optional extraction history for initial estimates
            verbose_timing: Show detailed timing information
            logger: Optional logger instance
            output: Optional UserOutput instance for user-facing messages
        """
        self.extraction_history = extraction_history
        self.verbose_timing = verbose_timing
        self.logger = logger
        self.output = output

        # Current extraction tracking
        self._total_chunks = 0
        self._completed_chunks = 0
        self._start_time = 0.0
        self._chunk_times: list[float] = []
        self._estimated_total_rows = 0

        # Batch tracking
        self._total_pmus = 0
        self._completed_pmus = 0
        self._batch_start_time = 0.0
        self._current_pmu_id: int | None = None

        # Spinner and display thread
        self._spinner = Spinner()
        self._display_thread: threading.Thread | None = None
        self._display_running = False
        self._display_paused = False
        self._display_lock = threading.Lock()
        self._last_eta = "Calculating ETA..."

        # Only show progress in interactive terminals
        self._is_tty = sys.stdout.isatty()

    def start_extraction(
        self, total_chunks: int, pmu_id: int | None = None, estimated_rows: int = 0
    ) -> None:
        """
        Start tracking a new extraction.

        Args:
            total_chunks: Total number of chunks to process
            pmu_id: PMU ID being extracted
            estimated_rows: Estimated total rows (if known)
        """
        self._total_chunks = total_chunks
        self._completed_chunks = 0
        self._start_time = time.time()
        self._chunk_times = []
        self._current_pmu_id = pmu_id
        self._estimated_total_rows = estimated_rows
        self._last_eta = "Calculating ETA..."

        # Start spinner and display thread
        self._spinner.start()
        self._start_display_thread()

    def start_batch(self, total_pmus: int) -> None:
        """
        Start tracking a batch extraction.

        Args:
            total_pmus: Total number of PMUs to process
        """
        self._total_pmus = total_pmus
        self._completed_pmus = 0
        self._batch_start_time = time.time()

    def update_chunk_progress(
        self,
        chunk_index: int,
        rows_in_chunk: int = 0,  # noqa: ARG002 - Reserved for future use
    ) -> None:
        """
        Update progress after a chunk completes.

        Args:
            chunk_index: Index of the completed chunk (0-based)
            rows_in_chunk: Number of rows in the completed chunk
        """
        with self._display_lock:
            self._completed_chunks = chunk_index + 1
            current_time = time.time()
            elapsed = current_time - self._start_time
            self._chunk_times.append(elapsed)

            # Calculate and store ETA (will be displayed by display thread)
            self._last_eta = self._calculate_eta()

            # Log to logger if available
            if self.logger and self.verbose_timing and self._chunk_times:
                last_chunk_time = self._chunk_times[-1] - (
                    self._chunk_times[-2] if len(self._chunk_times) > 1 else 0
                )
                self.logger.debug(
                    f"Chunk {self._completed_chunks}/{self._total_chunks} completed in {last_chunk_time:.2f}s"
                )

    def update_pmu_progress(self, pmu_index: int, pmu_id: int) -> None:
        """
        Update progress after a PMU completes in batch extraction.

        Args:
            pmu_index: Index of the completed PMU (0-based)
            pmu_id: ID of the completed PMU
        """
        self._completed_pmus = pmu_index + 1

        if not self._is_tty:
            return

        # Pause display to print batch messages cleanly
        with self._display_lock:
            # Clear the current line
            print("\r" + " " * 120, end="", flush=True)
            print()  # Move to new line

            if self.output:
                self.output.batch_progress(self._completed_pmus, self._total_pmus, pmu_id)

            # Calculate batch ETA if we have enough data
            if self._completed_pmus > 0 and self._total_pmus > self._completed_pmus:
                elapsed = time.time() - self._batch_start_time
                avg_time_per_pmu = elapsed / self._completed_pmus
                remaining_pmus = self._total_pmus - self._completed_pmus
                remaining_time = avg_time_per_pmu * remaining_pmus
                eta_str = self._format_time(remaining_time)

                progress_pct = self._completed_pmus / self._total_pmus * 100
                if self.output:
                    self.output.info(
                        f"Overall progress: {self._completed_pmus}/{self._total_pmus} ({progress_pct:.0f}%) | ETA: {eta_str}",
                        tag="BATCH",
                    )

    def finish_extraction(self) -> None:
        """Mark extraction as complete and print final message."""
        # Stop display thread and spinner
        self._stop_display_thread()
        self._spinner.stop()

        # Always clear the progress line and move to new line if we were displaying progress
        if self._is_tty and self._total_chunks > 0:
            print("\r" + " " * 120, end="", flush=True)  # Clear any remaining progress text
            print()  # Move to new line

        if self._completed_chunks > 0 and self._is_tty:
            elapsed = time.time() - self._start_time
            elapsed_str = self._format_time(elapsed)

            pmu_label = f"PMU {self._current_pmu_id}" if self._current_pmu_id else "Extraction"
            if self.output:
                self.output.info(
                    f"Completed {self._total_chunks} chunks in {elapsed_str}", tag=pmu_label
                )

    def finish_batch(self) -> None:
        """Mark batch extraction as complete."""
        if self._completed_pmus > 0 and self._is_tty:
            elapsed = time.time() - self._batch_start_time
            elapsed_str = self._format_time(elapsed)
            # Clear any remaining text on current line
            print("\r" + " " * 120, end="", flush=True)
            print()  # Move to new line
            if self.output:
                self.output.info(
                    f"Completed all {self._total_pmus} PMUs in {elapsed_str}", tag="BATCH"
                )

    def pause_display(self) -> None:
        """Temporarily pause the progress display updates."""
        with self._display_lock:
            if self._is_tty:
                # Clear the current progress line before pausing
                print("\r" + " " * 120, end="", flush=True)
                print()  # Move to new line
            self._display_paused = True

    def resume_display(self) -> None:
        """Resume the progress display updates."""
        with self._display_lock:
            self._display_paused = False

    def _start_display_thread(self) -> None:
        """Start background thread for updating display with spinner and elapsed time."""
        self._display_running = True
        self._display_thread = threading.Thread(target=self._display_loop, daemon=True)
        self._display_thread.start()

    def _stop_display_thread(self) -> None:
        """Stop display thread and wait for it to finish."""
        self._display_running = False
        if self._display_thread and self._display_thread.is_alive():
            self._display_thread.join(timeout=1.0)
        self._display_thread = None

    def _display_loop(self) -> None:
        """
        Background thread loop that updates display with spinner and elapsed time.

        Updates every 250ms with current spinner frame and elapsed time.
        """
        from .signal_handler import get_cancellation_manager  # noqa: PLC0415

        cancellation_manager = get_cancellation_manager()
        update_interval = 0.25  # 250ms

        while self._display_running:
            # Check for cancellation
            if cancellation_manager.is_cancelled():
                with self._display_lock:
                    # Clear the progress line and add newline
                    print("\r" + " " * 120, end="")
                    print()
                    self._display_running = False
                break

            # Update display
            self._update_display()

            # Sleep for update interval
            time.sleep(update_interval)

    def _update_display(self) -> None:
        """Update the progress display with spinner, elapsed time, and ETA."""
        with self._display_lock:
            if self._total_chunks == 0 or self._display_paused or not self._is_tty:
                return

            # Get current state
            spinner_frame = self._spinner.current_frame()
            elapsed = time.time() - self._start_time
            elapsed_str = self._format_time(elapsed)
            progress_pct = (
                (self._completed_chunks / self._total_chunks * 100) if self._total_chunks > 0 else 0
            )

            # Format message
            pmu_label = f"PMU {self._current_pmu_id}" if self._current_pmu_id else "Extraction"
            message = f"\r[{pmu_label}] Chunk {self._completed_chunks}/{self._total_chunks} ({progress_pct:.0f}%) {spinner_frame} Elapsed: {elapsed_str} | {self._last_eta}"

            # Add verbose timing if enabled
            if self.verbose_timing and self._chunk_times and len(self._chunk_times) > 0:
                last_chunk_time = self._chunk_times[-1] - (
                    self._chunk_times[-2] if len(self._chunk_times) > 1 else 0
                )
                message += f" | Last chunk: {self._format_time(last_chunk_time)}"

            # Pad message with spaces to clear any leftover characters (single print to avoid flicker)
            message = message.ljust(120)
            print(message, end="", flush=True)

    def _calculate_eta(self) -> str:
        """Calculate and format ETA string."""
        # Need at least 1 chunk if we have history, or 3 chunks without history
        min_chunks_needed = (
            1
            if (self.extraction_history and self.extraction_history.get_history_count() > 0)
            else 3
        )

        if self._completed_chunks < min_chunks_needed:
            return "Calculating ETA..."

        remaining_chunks = self._total_chunks - self._completed_chunks
        if remaining_chunks <= 0:
            return "ETA: 0s"

        # Calculate current run average
        current_elapsed = time.time() - self._start_time
        current_avg_time_per_chunk = current_elapsed / self._completed_chunks

        # If we have enough current data (3+ chunks), use weighted average
        if self._completed_chunks >= 3:
            # Use 70% current run, 30% historical if available
            if self.extraction_history and self.extraction_history.get_history_count() > 0:
                historical_avg = self.extraction_history.get_average_rows_per_sec()
                if historical_avg and self._estimated_total_rows > 0:
                    # Estimate based on historical throughput
                    historical_time_per_chunk = (
                        self._estimated_total_rows / self._total_chunks
                    ) / historical_avg
                    # Weighted average
                    avg_time_per_chunk = (0.7 * current_avg_time_per_chunk) + (
                        0.3 * historical_time_per_chunk
                    )
                else:
                    avg_time_per_chunk = current_avg_time_per_chunk
            else:
                avg_time_per_chunk = current_avg_time_per_chunk
        # Use historical average if available, otherwise current
        elif self.extraction_history and self.extraction_history.get_history_count() > 0:
            historical_avg = self.extraction_history.get_average_rows_per_sec()
            if historical_avg and self._estimated_total_rows > 0:
                avg_time_per_chunk = (
                    self._estimated_total_rows / self._total_chunks
                ) / historical_avg
            else:
                avg_time_per_chunk = current_avg_time_per_chunk
        else:
            avg_time_per_chunk = current_avg_time_per_chunk

        remaining_time = remaining_chunks * avg_time_per_chunk
        return f"ETA: {self._format_time(remaining_time)}"

    @staticmethod
    def _format_time(seconds: float) -> str:
        """
        Format time duration in human-readable format.

        Args:
            seconds: Time in seconds

        Returns:
            Formatted string (e.g., "2m 15s", "45s", "1h 23m")
        """
        if seconds < 0:
            return "0s"

        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)

        if hours > 0:
            return f"{hours}h {minutes}m"
        if minutes > 0:
            return f"{minutes}m {secs}s"
        return f"{secs}s"


class ScanProgressTracker:
    """Track and display table scanning progress with spinner."""

    def __init__(self):
        """Initialize scan progress tracker."""
        self._spinner = Spinner()
        self._start_time = 0.0
        self._display_thread: threading.Thread | None = None
        self._display_running = False
        self._display_lock = threading.Lock()

        # Current scan state
        self._completed = 0
        self._total = 0
        self._found_count = 0

        # Only show progress in interactive terminals
        self._is_tty = sys.stdout.isatty()

    def start(self) -> None:
        """Start tracking table scan."""
        self._start_time = time.time()
        self._spinner.start()
        self._start_display_thread()

    def stop(self) -> None:
        """Stop tracking and display final message."""
        self._stop_display_thread()
        self._spinner.stop()

        # Always clear the progress line and move to new line if we were displaying progress
        if self._is_tty and self._total > 0:
            print("\r" + " " * 120, end="", flush=True)
            print()

    def update(self, completed: int, total: int, found_count: int) -> None:
        """
        Update scan progress.

        Args:
            completed: Number of tables checked so far
            total: Total number of tables to check
            found_count: Number of tables found so far
        """
        with self._display_lock:
            self._completed = completed
            self._total = total
            self._found_count = found_count

            # If scan is complete, stop and show final message
            if completed >= total:
                # Stop display updates
                self._display_running = False

    def finish(self) -> None:
        """Display final completion message."""
        self.stop()

        if not self._is_tty:
            return

        # Print final message (line already cleared by stop())
        percentage = 100 if self._total > 0 else 0
        print(
            f"Scanning: {self._completed}/{self._total} ({percentage}%) - {self._found_count} tables found ✓"
        )

    def _start_display_thread(self) -> None:
        """Start background thread for updating display."""
        self._display_running = True
        self._display_thread = threading.Thread(target=self._display_loop, daemon=True)
        self._display_thread.start()

    def _stop_display_thread(self) -> None:
        """Stop display thread."""
        self._display_running = False
        if self._display_thread and self._display_thread.is_alive():
            self._display_thread.join(timeout=1.0)
        self._display_thread = None

    def _display_loop(self) -> None:
        """Background thread loop for updating display."""
        from .signal_handler import get_cancellation_manager  # noqa: PLC0415

        cancellation_manager = get_cancellation_manager()
        update_interval = 0.25  # 250ms

        while self._display_running:
            # Check for cancellation
            if cancellation_manager.is_cancelled():
                with self._display_lock:
                    # Clear the progress line and add newline
                    print("\r" + " " * 120, end="")
                    print()
                    self._display_running = False
                break

            # Update display
            self._update_display()

            # Sleep
            time.sleep(update_interval)

    def _update_display(self) -> None:
        """Update the scan progress display."""
        with self._display_lock:
            if self._total == 0 or not self._is_tty:
                return

            spinner_frame = self._spinner.current_frame()
            elapsed = time.time() - self._start_time
            elapsed_str = ProgressTracker._format_time(elapsed)
            percentage = int((self._completed / self._total) * 100) if self._total > 0 else 0

            message = f"\rScanning: {self._completed}/{self._total} ({percentage}%) {spinner_frame} Elapsed: {elapsed_str} - {self._found_count} tables found..."
            # Pad message with spaces to clear any leftover characters (single print to avoid flicker)
            message = message.ljust(120)
            print(message, end="", flush=True)
