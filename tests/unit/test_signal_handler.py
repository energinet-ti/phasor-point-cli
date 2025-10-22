"""
Unit tests for the CancellationManager and signal handling.
"""

import signal
from unittest.mock import MagicMock

import pytest

from phasor_point_cli.signal_handler import CancellationManager, get_cancellation_manager


@pytest.fixture
def cancellation_manager():
    """Create a fresh CancellationManager instance for testing."""
    manager = CancellationManager()
    manager.reset()
    return manager


def test_cancellation_manager_singleton():
    """Test that CancellationManager is a singleton."""
    # Arrange & Act
    manager1 = get_cancellation_manager()
    manager2 = get_cancellation_manager()

    # Assert
    assert manager1 is manager2


def test_is_cancelled_initially_false(cancellation_manager):
    """Test that is_cancelled returns False initially."""
    # Act & Assert
    assert cancellation_manager.is_cancelled() is False


def test_cancel_sets_cancelled_flag(cancellation_manager):
    """Test that cancel() sets the cancelled flag."""
    # Act
    cancellation_manager.cancel()

    # Assert
    assert cancellation_manager.is_cancelled() is True


def test_reset_clears_cancelled_flag(cancellation_manager):
    """Test that reset() clears the cancelled flag."""
    # Arrange
    cancellation_manager.cancel()
    assert cancellation_manager.is_cancelled() is True

    # Act
    cancellation_manager.reset()

    # Assert
    assert cancellation_manager.is_cancelled() is False


def test_set_logger(cancellation_manager):
    """Test that set_logger stores the logger."""
    # Arrange
    logger = MagicMock()

    # Act
    cancellation_manager.set_logger(logger)

    # Assert
    assert cancellation_manager._logger is logger


def test_signal_handler_first_ctrl_c(cancellation_manager):
    """Test that first Ctrl+C sets cancelled flag."""
    # Arrange
    logger = MagicMock()
    cancellation_manager.set_logger(logger)

    # Act
    cancellation_manager._signal_handler(signal.SIGINT, None)

    # Assert
    assert cancellation_manager.is_cancelled() is True
    logger.warning.assert_called()


def test_signal_handler_second_ctrl_c_exits(cancellation_manager):
    """Test that second Ctrl+C forces exit."""
    # Arrange
    logger = MagicMock()
    cancellation_manager.set_logger(logger)
    cancellation_manager.cancel()  # Simulate first Ctrl+C

    # Act & Assert
    with pytest.raises(SystemExit) as exc_info:
        cancellation_manager._signal_handler(signal.SIGINT, None)

    assert exc_info.value.code == 1
    logger.warning.assert_called()


def test_register_signal_handler(cancellation_manager):
    """Test that register_signal_handler sets up SIGINT handler."""
    # Arrange
    original_handler = signal.getsignal(signal.SIGINT)

    # Act
    cancellation_manager.register_signal_handler()
    new_handler = signal.getsignal(signal.SIGINT)

    # Assert
    assert new_handler == cancellation_manager._signal_handler
    assert cancellation_manager._original_sigint_handler == original_handler

    # Cleanup
    cancellation_manager.unregister_signal_handler()


def test_unregister_signal_handler(cancellation_manager):
    """Test that unregister_signal_handler restores original handler."""
    # Arrange
    original_handler = signal.getsignal(signal.SIGINT)
    cancellation_manager.register_signal_handler()

    # Act
    cancellation_manager.unregister_signal_handler()
    restored_handler = signal.getsignal(signal.SIGINT)

    # Assert
    assert restored_handler == original_handler
    assert cancellation_manager._original_sigint_handler is None


def test_context_manager_enter_resets_and_registers(cancellation_manager):
    """Test that context manager __enter__ resets state and registers handler."""
    # Arrange
    cancellation_manager.cancel()  # Set cancelled flag

    # Act
    with cancellation_manager:
        # Assert
        assert cancellation_manager.is_cancelled() is False
        current_handler = signal.getsignal(signal.SIGINT)
        assert current_handler == cancellation_manager._signal_handler


def test_context_manager_exit_unregisters(cancellation_manager):
    """Test that context manager __exit__ unregisters handler."""
    # Arrange
    original_handler = signal.getsignal(signal.SIGINT)

    # Act
    with cancellation_manager:
        pass  # Just enter and exit

    # Assert
    restored_handler = signal.getsignal(signal.SIGINT)
    assert restored_handler == original_handler


def test_context_manager_with_exception(cancellation_manager):
    """Test that context manager properly cleans up even on exception."""
    # Arrange
    original_handler = signal.getsignal(signal.SIGINT)

    # Act & Assert
    with pytest.raises(ValueError):
        with cancellation_manager:
            raise ValueError("Test exception")

    # Assert handler is restored
    restored_handler = signal.getsignal(signal.SIGINT)
    assert restored_handler == original_handler


def test_thread_safety_cancel(cancellation_manager):
    """Test that cancel() is thread-safe."""
    import threading

    # Arrange
    results = []

    def cancel_worker():
        cancellation_manager.cancel()
        results.append(cancellation_manager.is_cancelled())

    # Act
    threads = [threading.Thread(target=cancel_worker) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # Assert
    assert all(results)  # All threads should see cancelled=True
    assert cancellation_manager.is_cancelled() is True


def test_thread_safety_reset(cancellation_manager):
    """Test that reset() is thread-safe."""
    import threading

    # Arrange
    cancellation_manager.cancel()

    def reset_worker():
        cancellation_manager.reset()

    # Act
    threads = [threading.Thread(target=reset_worker) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # Assert
    assert cancellation_manager.is_cancelled() is False
