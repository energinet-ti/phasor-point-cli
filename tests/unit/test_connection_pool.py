"""
Unit tests for the enhanced JDBCConnectionPool class.
"""

from __future__ import annotations

import pytest

from phasor_point_cli.connection_pool import JDBCConnectionPool


def create_pool(mocker, max_connections=2):
    # Mock pyodbc.connect for the lazy import in get_connection
    mocker.patch("pyodbc.connect")
    return JDBCConnectionPool("DSN=Test", max_connections=max_connections)


def test_pool_properties_reflect_state(mocker):
    # Arrange
    pool = create_pool(mocker, max_connections=3)

    # Act & Assert
    assert pool.pool_size == 3
    assert pool.available_connections == 0


def test_pool_resize_increase_and_decrease(mocker):
    # Arrange
    pool = create_pool(mocker, max_connections=2)

    # Act
    pool.resize(4)

    # Assert
    assert pool.pool_size == 4

    # Arrange - populate pool with two mock connections
    first = mocker.MagicMock()
    second = mocker.MagicMock()
    pool.pool.extend([first, second])

    # Act
    pool.resize(1)

    # Assert
    assert pool.pool_size == 1
    assert len(pool.pool) == 1  # One connection should have been closed and removed
    second.close.assert_called_once()


def test_pool_resize_rejects_invalid_size(mocker):
    # Arrange
    pool = create_pool(mocker)

    # Act & Assert
    with pytest.raises(ValueError):
        pool.resize(0)


def test_available_connections_updates_after_return(mocker):
    # Arrange
    pool = create_pool(mocker)
    mock_conn = mocker.MagicMock()
    pool.pool.append(mock_conn)

    # Act
    conn = pool.get_connection()

    # Assert
    assert conn is mock_conn
    assert pool.available_connections == 0

    # Act
    pool.return_connection(conn)

    # Assert
    assert pool.available_connections == 1


def test_pool_returns_none_when_pyodbc_unavailable(mocker):
    """Test that pool returns None gracefully when pyodbc import fails."""
    # Note: The actual pool exhaustion scenario is tested via connection creation failure
    # The current pool implementation doesn't track "in use" connections separately
    # from idle connections, so we test the error path instead

    # Arrange
    mock_pyodbc = mocker.patch("pyodbc.connect")
    mock_pyodbc.side_effect = ImportError("pyodbc not installed")
    mock_logger = mocker.MagicMock()

    pool = JDBCConnectionPool("DSN=Test", max_connections=2, logger=mock_logger)

    # Act
    conn = pool.get_connection()

    # Assert
    assert conn is None


def test_connection_creation_failure_returns_none(mocker):
    """Test that connection creation failure returns None and logs error."""
    # Arrange
    mock_pyodbc = mocker.patch("pyodbc.connect")
    mock_pyodbc.side_effect = Exception("Database unreachable")

    mock_logger = mocker.MagicMock()
    pool = JDBCConnectionPool("DSN=Test", max_connections=2, logger=mock_logger)

    # Act
    conn = pool.get_connection()

    # Assert
    assert conn is None
    mock_logger.error.assert_called_once()
    assert "Failed to create connection" in str(mock_logger.error.call_args)


def test_connection_close_error_is_handled(mocker):
    """Test that connection.close() errors don't crash during cleanup."""
    # Arrange
    pool = create_pool(mocker)
    mock_conn = mocker.MagicMock()
    mock_conn.close.side_effect = Exception("Close failed")
    pool.pool.append(mock_conn)

    # Act - Should not raise exception
    pool.cleanup()

    # Assert - Connection should still be removed from pool
    assert len(pool.pool) == 0


def test_return_connection_when_pool_full_closes_connection(mocker):
    """Test that returning a connection when pool is full closes it."""
    # Arrange
    pool = create_pool(mocker, max_connections=2)

    # Fill the pool
    conn1 = mocker.MagicMock()
    conn2 = mocker.MagicMock()
    pool.pool = [conn1, conn2]

    # Act - Try to return another connection when pool is full
    extra_conn = mocker.MagicMock()
    pool.return_connection(extra_conn)

    # Assert - Extra connection should be closed, not added to pool
    extra_conn.close.assert_called_once()
    assert len(pool.pool) == 2  # Pool size unchanged


def test_cleanup_with_close_errors_continues(mocker):
    """Test that cleanup continues even if some connections fail to close."""
    # Arrange
    pool = create_pool(mocker)
    mock_logger = mocker.MagicMock()
    pool.logger = mock_logger

    conn1 = mocker.MagicMock()
    conn2 = mocker.MagicMock()
    conn3 = mocker.MagicMock()

    # Make middle connection fail to close
    conn2.close.side_effect = Exception("Close error")

    pool.pool = [conn1, conn2, conn3]

    # Act
    pool.cleanup()

    # Assert - All connections attempted to close, pool is empty
    conn1.close.assert_called_once()
    conn2.close.assert_called_once()
    conn3.close.assert_called_once()
    assert len(pool.pool) == 0


def test_resize_negative_size_raises_error(mocker):
    """Test that resizing to negative size raises ValueError."""
    # Arrange
    pool = create_pool(mocker)

    # Act & Assert
    with pytest.raises(ValueError) as exc_info:
        pool.resize(-1)

    assert "must be positive" in str(exc_info.value)


def test_resize_with_close_errors_continues(mocker):
    """Test that resize continues even if connection close fails."""
    # Arrange
    pool = create_pool(mocker, max_connections=3)
    mock_logger = mocker.MagicMock()
    pool.logger = mock_logger

    # Add connections to pool - resize pops from end, so conn2 closed first, then conn1
    conn1 = mocker.MagicMock()
    conn2 = mocker.MagicMock()
    conn2.close.side_effect = Exception("Close failed")  # Make second one fail

    pool.pool = [conn1, conn2]

    # Act - Resize down from 3 to 1, should close 1 connection (pool has 2, new size is 1)
    pool.resize(1)

    # Assert - Resize completes even with close error
    assert pool.pool_size == 1
    assert len(pool.pool) == 1
    conn2.close.assert_called_once()  # Popped and attempted to close (failed)
    # conn1 stays in pool since we only needed to remove one connection
    assert conn1 in pool.pool
