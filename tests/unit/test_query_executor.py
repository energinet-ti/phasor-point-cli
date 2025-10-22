"""
Unit tests for the QueryExecutor class.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from phasor_point_cli.query_executor import QueryExecutor


def test_execute_success(tmp_path, monkeypatch):
    # Arrange
    connection = MagicMock()
    pool = MagicMock()
    pool.get_connection.return_value = connection
    logger = MagicMock()

    df = pd.DataFrame({"value": [1, 2, 3]})
    monkeypatch.setattr("pandas.read_sql_query", lambda query, conn, params=None: df)

    executor = QueryExecutor(pool, logger)
    output_file = tmp_path / "results.csv"

    # Act
    result = executor.execute(
        "SELECT * FROM test", output_file=str(output_file), output_format="csv"
    )

    # Assert
    assert result.success is True
    assert result.rows_returned == 3
    assert result.output_file == output_file
    assert output_file.exists()
    pool.return_connection.assert_called_once_with(connection)


def test_execute_handles_error(monkeypatch):
    # Arrange
    pool = MagicMock()
    pool.get_connection.return_value = MagicMock()
    logger = MagicMock()

    def raise_error(query, conn, params=None):
        raise Exception("boom")

    monkeypatch.setattr("pandas.read_sql_query", raise_error)
    executor = QueryExecutor(pool, logger)

    # Act
    result = executor.execute("SELECT * FROM broken")

    # Assert
    assert result.success is False
    assert result.error is not None and "boom" in result.error


def test_execute_handles_connection_failure():
    """Test that execute handles None connection from pool gracefully."""
    # Arrange
    pool = MagicMock()
    pool.get_connection.return_value = None  # Pool exhausted or connection failed
    logger = MagicMock()

    executor = QueryExecutor(pool, logger)

    # Act
    result = executor.execute("SELECT * FROM test")

    # Assert
    assert result.success is False
    assert result.error is not None and "Unable to obtain connection" in result.error
    pool.return_connection.assert_not_called()  # No connection to return


def test_execute_with_cursor_fallback_on_none_result(monkeypatch):
    """Test cursor fallback when pandas.read_sql_query returns None."""
    # Arrange
    connection = MagicMock()
    cursor = MagicMock()
    cursor.description = [("col1",), ("col2",)]
    cursor.fetchall.return_value = [(1, 2), (3, 4)]
    connection.cursor.return_value = cursor

    pool = MagicMock()
    pool.get_connection.return_value = connection
    logger = MagicMock()

    # Return None to trigger cursor fallback
    monkeypatch.setattr("pandas.read_sql_query", lambda q, c, params=None: None)

    executor = QueryExecutor(pool, logger)

    # Act - Use empty output_format to prevent file creation
    result = executor.execute("SELECT * FROM test", output_format="")

    # Assert - Should succeed using cursor fallback
    assert result.success is True
    assert result.rows_returned == 2
    cursor.execute.assert_called_once()
    connection.cursor.assert_called()


def test_execute_cursor_fallback_non_query_statement(monkeypatch):
    """Test cursor fallback for non-query statements (INSERT, UPDATE, DELETE)."""
    # Arrange
    connection = MagicMock()
    cursor = MagicMock()
    cursor.description = None  # Non-query statement has no description
    connection.cursor.return_value = cursor

    pool = MagicMock()
    pool.get_connection.return_value = connection
    logger = MagicMock()

    # Return None to trigger cursor fallback
    monkeypatch.setattr("pandas.read_sql_query", lambda q, c, params=None: None)

    executor = QueryExecutor(pool, logger)

    # Act
    result = executor.execute("UPDATE test SET value = 1")

    # Assert - Should succeed with 0 rows returned
    assert result.success is True
    assert result.rows_returned == 0


def test_execute_handles_query_with_database_error(monkeypatch):
    """Test handling of database errors during query execution."""
    # Arrange
    connection = MagicMock()
    pool = MagicMock()
    pool.get_connection.return_value = connection
    logger = MagicMock()

    def raise_error(query, conn, params=None):
        raise Exception("Table does not exist")

    monkeypatch.setattr("pandas.read_sql_query", raise_error)
    executor = QueryExecutor(pool, logger)

    # Act
    result = executor.execute("SELECT * FROM nonexistent")

    # Assert
    assert result.success is False
    assert result.error is not None and "Table does not exist" in result.error


def test_execute_returns_connection_on_exception(monkeypatch):
    """Test that connection is returned to pool even when exception occurs."""
    # Arrange
    connection = MagicMock()
    pool = MagicMock()
    pool.get_connection.return_value = connection
    logger = MagicMock()

    def raise_error(query, conn, params=None):
        raise Exception("Query failed")

    monkeypatch.setattr("pandas.read_sql_query", raise_error)
    executor = QueryExecutor(pool, logger)

    # Act
    result = executor.execute("SELECT * FROM test")

    # Assert
    assert result.success is False
    pool.return_connection.assert_called_once_with(connection)


def test_execute_with_unsupported_output_format(monkeypatch):
    """Test handling of unsupported output format."""
    # Arrange
    connection = MagicMock()
    pool = MagicMock()
    pool.get_connection.return_value = connection
    logger = MagicMock()

    df = pd.DataFrame({"value": [1, 2, 3]})
    monkeypatch.setattr("pandas.read_sql_query", lambda q, c, params=None: df)

    executor = QueryExecutor(pool, logger)

    # Act - Execute with unsupported format
    result = executor.execute("SELECT * FROM test", output_format="xlsx", output_file="test.xlsx")

    # Assert - Query succeeds but file save should fail gracefully
    assert result.success is True
    assert result.rows_returned == 3
    assert result.output_file is None  # File save failed, but query succeeded
    logger.error.assert_called()  # Error logged for file save failure
