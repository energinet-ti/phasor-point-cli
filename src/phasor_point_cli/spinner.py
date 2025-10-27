"""
Thread-based spinner animation for visual activity feedback.

Provides an animated spinner that runs in a background thread to show
that long-running operations are still active.
"""

from __future__ import annotations

import sys
import threading
import time


class Spinner:
    """Thread-based spinner for visual activity feedback during long operations."""

    # Unicode braille dots spinner - works on most modern terminals
    FRAMES_UNICODE = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

    # ASCII fallback for terminals that don't support Unicode
    FRAMES_ASCII = ["|", "/", "-", "\\"]

    def __init__(self, *, use_unicode: bool | None = None):
        """
        Initialize the spinner.

        Args:
            use_unicode: Whether to use Unicode spinner. If None, auto-detect.
        """
        self._frame_index = 0
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

        # Auto-detect Unicode support if not specified
        if use_unicode is None:
            use_unicode = self._supports_unicode()

        self._frames = self.FRAMES_UNICODE if use_unicode else self.FRAMES_ASCII
        self._update_interval = 0.1  # 100ms = 10 fps

    @staticmethod
    def _supports_unicode() -> bool:
        """
        Check if terminal supports Unicode.

        Returns:
            True if terminal likely supports Unicode, False otherwise
        """
        # Check if stdout is a TTY
        if not sys.stdout.isatty():
            return False

        # Check encoding
        encoding = sys.stdout.encoding or ""
        return encoding.lower() in ("utf-8", "utf8")

    def start(self) -> None:
        """Start the spinner animation in a background thread."""
        with self._lock:
            if self._running:
                # Already running, don't start again
                return

            self._running = True
            self._frame_index = 0
            self._thread = threading.Thread(target=self._spin_loop, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        """Stop the spinner animation and wait for thread to finish."""
        with self._lock:
            if not self._running:
                # Already stopped
                return

            self._running = False

        # Wait for thread to finish (outside lock to avoid deadlock)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)

        self._thread = None

    def current_frame(self) -> str:
        """
        Get the current spinner frame.

        Returns:
            Current spinner character
        """
        with self._lock:
            if not self._running:
                return " "  # Return space if not running
            return self._frames[self._frame_index]

    def _spin_loop(self) -> None:
        """
        Background thread loop that rotates the spinner frames.

        Checks for cancellation from CancellationManager to stop gracefully.
        """
        from .signal_handler import get_cancellation_manager  # noqa: PLC0415

        cancellation_manager = get_cancellation_manager()

        while True:
            # Check if we should stop
            with self._lock:
                if not self._running:
                    break

            # Check for user cancellation (Ctrl+C)
            if cancellation_manager.is_cancelled():
                with self._lock:
                    self._running = False
                break

            # Rotate to next frame
            with self._lock:
                self._frame_index = (self._frame_index + 1) % len(self._frames)

            # Sleep for update interval
            time.sleep(self._update_interval)

    def __enter__(self) -> Spinner:
        """Context manager entry - start spinner."""
        self.start()
        return self

    def __exit__(self, *args) -> None:
        """Context manager exit - stop spinner."""
        self.stop()
