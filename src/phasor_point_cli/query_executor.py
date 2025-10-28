"""
Query execution utilities for the refactored CLI.
"""

from __future__ import annotations

import time
import warnings
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path

import pandas as pd

from .models import QueryResult


class QueryExecutor:
    """Execute ad-hoc SQL queries against the connection pool."""

    def __init__(self, connection_pool, logger):
        self.connection_pool = connection_pool
        self.logger = logger

    def execute(  # noqa: PLR0912, PLR0915
        self,
        query: str,
        *,
        params: Sequence | None = None,
        output_file: str | None = None,
        output_format: str = "parquet",
        preview_rows: int = 5,
    ) -> QueryResult:
        start_clock = time.monotonic()
        self.logger.info("Executing custom query...")
        self.logger.debug("Query: %s", query)

        conn = self.connection_pool.get_connection()
        if not conn:
            return QueryResult(
                success=False,
                rows_returned=0,
                duration_seconds=0.0,
                error="Unable to obtain connection",
            )

        # Clear any pending state on the connection for custom JDBC
        with suppress(Exception):
            conn.commit()

        df = None
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")
                # Type ignore for params - pandas accepts various sequence types
                df = pd.read_sql_query(query, conn, params=params)  # type: ignore[arg-type]

            # Handle case where read_sql_query returns None for custom JDBC
            if df is None:
                # Try using cursor directly for aggregate queries
                cursor = conn.cursor()
                try:
                    cursor.execute(query)
                    # Check if this is a query that returns results
                    if cursor.description:
                        columns: list[str] = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        df = pd.DataFrame(rows, columns=columns)  # type: ignore[arg-type]
                    else:
                        # Non-query statement (e.g., INSERT, UPDATE)
                        df = pd.DataFrame()
                finally:
                    with suppress(Exception):
                        cursor.close()

        except Exception as exc:
            self.logger.error("Error executing query: %s", exc)
            self.logger.debug("Query failure details", exc_info=True)
            return QueryResult(
                success=False,
                rows_returned=0,
                duration_seconds=time.monotonic() - start_clock,
                error=str(exc),
            )
        finally:
            # Clear connection state before returning to pool
            with suppress(Exception):
                conn.commit()
            self.connection_pool.return_connection(conn)

        rows = len(df) if df is not None else 0
        cols = len(df.columns) if df is not None and not df.empty else 0
        duration = time.monotonic() - start_clock
        self.logger.info("Query executed successfully (%s rows, %s columns)", rows, cols)

        if rows > 0 and preview_rows and df is not None:
            print("\n[SEARCH] Sample results:")
            try:
                print(df.head(preview_rows).to_string(index=False))
            except Exception as exc:
                self.logger.warning("Unable to display preview: %s", exc)

        output_path = None
        if rows > 0 and output_format and df is not None:
            path = (
                Path(output_file)
                if output_file
                else Path(f"query_result_{int(time.time())}.{output_format}")
            )
            path = path.with_suffix(f".{output_format}")
            try:
                if output_format == "csv":
                    df.to_csv(path, index=False, encoding="utf-8")
                elif output_format == "parquet":
                    df.to_parquet(path, index=False)
                else:
                    raise ValueError(f"Unsupported output format '{output_format}'")
                file_size = path.stat().st_size / 1024 / 1024
                self.logger.info("Saved results to %s (%.2f MB)", path, file_size)
                output_path = path
            except Exception as exc:
                self.logger.error("Failed to save query results: %s", exc)
                output_path = None

        return QueryResult(
            success=True,
            rows_returned=rows,
            duration_seconds=duration,
            output_file=output_path,
        )
