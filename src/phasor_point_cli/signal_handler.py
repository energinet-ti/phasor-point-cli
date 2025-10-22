"""
Signal handling for graceful shutdown and cancellation.

Provides a mechanism to handle Ctrl+C (SIGINT) and other interrupt signals,
allowing long-running operations to be cancelled gracefully.
"""

import signal
import sys
import threading


class CancellationManager:
    """Thread-safe cancellation manager for handling interrupts."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """Singleton pattern to ensure only one instance exists."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize the cancellation manager."""
        if self._initialized:
            return

        self._cancelled = False
        self._original_sigint_handler = None
        self._lock = threading.Lock()
        self._logger = None
        self._initialized = True

    def set_logger(self, logger):
        """Set the logger for status messages."""
        self._logger = logger

    def is_cancelled(self) -> bool:
        """Check if cancellation has been requested."""
        with self._lock:
            return self._cancelled

    def cancel(self):
        """Mark operation as cancelled."""
        with self._lock:
            self._cancelled = True

    def reset(self):
        """Reset cancellation state."""
        with self._lock:
            self._cancelled = False

    def _signal_handler(self, _signum, _frame):
        """Handle interrupt signal (Ctrl+C)."""
        with self._lock:
            if self._cancelled:
                # Second Ctrl+C - force exit
                if self._logger:
                    self._logger.warning("\nForced exit - terminating immediately")
                print("\n\n[CANCELLED] Forced exit - terminating immediately", file=sys.stderr)
                sys.exit(1)
            else:
                # First Ctrl+C - request graceful cancellation
                self._cancelled = True
                if self._logger:
                    self._logger.warning(
                        "\nCancellation requested - finishing current operation..."
                    )
                print(
                    "\n\n[CANCELLED] Operation cancelled - cleaning up... (Press Ctrl+C again to force exit)",
                    file=sys.stderr,
                )

    def register_signal_handler(self):
        """Register the signal handler for SIGINT."""
        self._original_sigint_handler = signal.signal(signal.SIGINT, self._signal_handler)

    def unregister_signal_handler(self):
        """Restore the original signal handler."""
        if self._original_sigint_handler is not None:
            signal.signal(signal.SIGINT, self._original_sigint_handler)
            self._original_sigint_handler = None

    def __enter__(self):
        """Context manager entry - register signal handler and reset state."""
        self.reset()
        self.register_signal_handler()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - unregister signal handler."""
        self.unregister_signal_handler()
        return False


# Global singleton instance
_cancellation_manager = CancellationManager()


def get_cancellation_manager() -> CancellationManager:
    """Get the global cancellation manager instance."""
    return _cancellation_manager
