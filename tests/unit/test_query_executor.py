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
    assert "boom" in result.error
