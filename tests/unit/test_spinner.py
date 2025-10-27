"""
Tests for the Spinner class.

Tests spinner animation, thread management, context manager, and cancellation handling.
"""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from phasor_point_cli.spinner import Spinner


class TestSpinnerBasicFunctionality:
    """Test basic spinner operations."""

    def test_spinner_init(self):
        """Test spinner initializes correctly."""
        # Arrange & Act
        spinner = Spinner()

        # Assert
        assert not spinner._running
        assert spinner._frame_index == 0
        assert spinner._thread is None

    def test_spinner_start(self):
        """Test spinner starts and creates thread."""
        # Arrange
        spinner = Spinner()

        # Act
        spinner.start()

        # Assert
        assert spinner._running
        assert spinner._thread is not None
        assert spinner._thread.is_alive()

        # Cleanup
        spinner.stop()

    def test_spinner_stop(self):
        """Test spinner stops and thread terminates."""
        # Arrange
        spinner = Spinner()
        spinner.start()
        assert spinner._running

        # Act
        spinner.stop()

        # Assert
        assert not spinner._running
        # Give thread time to finish
        time.sleep(0.2)
        assert spinner._thread is None or not spinner._thread.is_alive()

    def test_spinner_frame_rotation(self):
        """Test spinner frames rotate through all frames."""
        # Arrange
        spinner = Spinner()
        spinner.start()

        # Act - collect several frames over time
        frames = []
        for _ in range(15):
            frames.append(spinner.current_frame())
            time.sleep(0.12)  # Wait for frame to change

        spinner.stop()

        # Assert - should have multiple different frames
        unique_frames = set(frames)
        assert len(unique_frames) >= 3  # At least 3 different frames seen

    def test_spinner_current_frame_before_start(self):
        """Test current_frame returns space when not running."""
        # Arrange
        spinner = Spinner()

        # Act
        frame = spinner.current_frame()

        # Assert
        assert frame == " "

    def test_spinner_idempotent_start(self):
        """Test starting spinner twice doesn't create duplicate threads."""
        # Arrange
        spinner = Spinner()
        spinner.start()
        first_thread = spinner._thread

        # Act
        spinner.start()  # Start again

        # Assert
        assert spinner._thread is first_thread  # Same thread

        # Cleanup
        spinner.stop()

    def test_spinner_idempotent_stop(self):
        """Test stopping spinner twice doesn't error."""
        # Arrange
        spinner = Spinner()
        spinner.start()

        # Act & Assert - should not raise
        spinner.stop()
        spinner.stop()  # Stop again


class TestSpinnerContextManager:
    """Test context manager functionality."""

    def test_spinner_context_manager_starts(self):
        """Test __enter__ starts spinner."""
        # Arrange
        spinner = Spinner()

        # Act
        with spinner:
            # Assert
            assert spinner._running
            assert spinner._thread is not None

    def test_spinner_context_manager_stops(self):
        """Test __exit__ stops spinner."""
        # Arrange
        spinner = Spinner()

        # Act
        with spinner:
            pass

        # Assert
        time.sleep(0.2)  # Give thread time to stop
        assert not spinner._running

    def test_spinner_context_manager_exception(self):
        """Test spinner still stops on exception."""
        # Arrange
        spinner = Spinner()

        # Act & Assert
        with pytest.raises(ValueError):
            with spinner:
                assert spinner._running
                raise ValueError("Test exception")

        # Wait for thread to stop
        time.sleep(0.2)
        assert not spinner._running


class TestSpinnerCancellation:
    """Test spinner respects cancellation."""

    def test_spinner_respects_cancellation(self):
        """Test spinner stops when CancellationManager.cancel() called."""
        # Arrange
        with patch("phasor_point_cli.signal_handler.get_cancellation_manager") as mock_get_cm:
            mock_cm = MagicMock()
            mock_cm.is_cancelled.return_value = False
            mock_get_cm.return_value = mock_cm

            spinner = Spinner()
            spinner.start()
            assert spinner._running

            # Act - simulate cancellation
            mock_cm.is_cancelled.return_value = True

            # Wait for spinner to check cancellation
            time.sleep(0.5)

            # Assert
            assert not spinner._running

    def test_spinner_cleanup_on_interrupt(self):
        """Test thread cleans up properly on interrupt."""
        # Arrange
        spinner = Spinner()
        spinner.start()
        thread = spinner._thread

        # Act
        spinner.stop()

        # Assert - thread should be cleaned up
        time.sleep(0.2)
        assert not thread.is_alive()


class TestSpinnerThreadSafety:
    """Test thread safety."""

    def test_spinner_concurrent_access(self):
        """Test multiple threads can read current_frame safely."""
        # Arrange
        spinner = Spinner()
        spinner.start()
        frames = []

        def read_frame():
            for _ in range(10):
                frames.append(spinner.current_frame())
                time.sleep(0.05)

        # Act
        threads = [threading.Thread(target=read_frame) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        spinner.stop()

        # Assert - should have collected frames without errors
        assert len(frames) == 30
        assert all(isinstance(f, str) for f in frames)

    def test_spinner_stop_while_updating(self):
        """Test can stop during frame update."""
        # Arrange
        spinner = Spinner()
        spinner.start()

        # Act - stop quickly after starting
        time.sleep(0.05)
        spinner.stop()

        # Assert - should stop cleanly
        time.sleep(0.2)
        assert not spinner._running


class TestSpinnerEdgeCases:
    """Test edge cases and special scenarios."""

    def test_spinner_unicode_support_detection(self):
        """Test Unicode support detection works."""
        # Arrange & Act
        mock_stdout = MagicMock()
        mock_stdout.isatty.return_value = True
        mock_stdout.encoding = "utf-8"

        with patch("sys.stdout", mock_stdout):
            result = Spinner._supports_unicode()

        # Assert
        assert result is True

    def test_spinner_no_unicode_fallback(self):
        """Test fallback to ASCII when Unicode not supported."""
        # Arrange & Act
        spinner = Spinner(use_unicode=False)

        # Assert
        assert spinner._frames == Spinner.FRAMES_ASCII

    def test_spinner_daemon_thread(self):
        """Test thread is daemon (won't block exit)."""
        # Arrange
        spinner = Spinner()

        # Act
        spinner.start()

        # Assert
        assert spinner._thread.daemon is True

        # Cleanup
        spinner.stop()

    def test_spinner_not_tty_no_unicode(self):
        """Test non-TTY defaults to no Unicode."""
        # Arrange & Act
        mock_stdout = MagicMock()
        mock_stdout.isatty.return_value = False

        with patch("sys.stdout", mock_stdout):
            result = Spinner._supports_unicode()

        # Assert
        assert result is False
