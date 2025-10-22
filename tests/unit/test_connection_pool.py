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
