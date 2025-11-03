"""
Table management services for PhasorPoint CLI.

The :class:`TableManager` encapsulates PMU table discovery, metadata retrieval,
and sampling logic for working with PMU data tables.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import suppress

import pandas as pd

from .models import PMUInfo, TableInfo, TableListResult, TableStatistics


class TableManagerError(Exception):
    """Raised when a table operation cannot be completed."""


class TableManager:
    """Manage discovery and inspection of PMU tables."""

    DEFAULT_RESOLUTIONS: tuple[int, ...] = (1, 10, 25, 50)

    def __init__(
        self, connection_pool, config_manager, logger: logging.Logger | None = None
    ) -> None:
        self.connection_pool = connection_pool
        self._config_manager = config_manager
        self.logger = logger or logging.getLogger("phasor_cli")
        self._pmu_lookup: dict[int, PMUInfo] | None = None

    # ------------------------------------------------------------------ Helpers
    def _get_config(self) -> dict:
        if hasattr(self._config_manager, "config"):
            return self._config_manager.config
        return self._config_manager or {}

    @classmethod
    def build_pmu_info_lookup(cls, config: dict) -> dict[int, PMUInfo]:
        lookup: dict[int, PMUInfo] = {}
        available = config.get("available_pmus", []) if config else []
        for entry in available:
            info = PMUInfo(
                id=int(entry["id"]),
                station_name=entry.get("station_name", "Unknown"),
                country=entry.get("country", ""),
            )
            lookup[info.id] = info
        return lookup

    def _ensure_pmu_lookup(self) -> dict[int, PMUInfo]:
        if self._pmu_lookup is None:
            self._pmu_lookup = self.build_pmu_info_lookup(self._get_config())
        return self._pmu_lookup

    # ------------------------------------------------------------ PMU Selection
    def determine_pmus_to_scan(
        self,
        pmu_ids: Sequence[int] | None,
        max_pmus: int | None,
    ) -> list[int] | None:
        if pmu_ids is not None:
            return list(pmu_ids)

        config = self._get_config()
        available = config.get("available_pmus", [])
        if not available:
            self.logger.warning("No PMUs available in configuration. Provide explicit PMU IDs.")
            return None

        all_ids: list[int] = [int(p["id"]) for p in available]

        if max_pmus is not None and len(all_ids) > max_pmus:
            self.logger.info(
                "Found %s PMUs, limiting scan to first %s entries", len(all_ids), max_pmus
            )
            return all_ids[:max_pmus]
        return all_ids

    # -------------------------------------------------------------- Connections
    def _acquire_connection(self):
        if self.connection_pool is None:
            raise TableManagerError("No connection pool available")
        conn = self.connection_pool.get_connection()
        if not conn:
            raise TableManagerError("Unable to obtain database connection")
        return conn

    # ----------------------------------------------------------- Table Scanning
    def _check_single_table(self, pmu_id: int, resolution: int) -> tuple[int, int] | None:
        """
        Check if a single table exists.

        Returns (pmu_id, resolution) tuple if table exists, None otherwise.
        This method is designed to be called concurrently from multiple threads.
        """
        table_name = f"pmu_{pmu_id}_{resolution}"
        conn = None
        cursor = None

        try:
            conn = self._acquire_connection()
            if not conn:
                return None

            cursor = conn.cursor()
            cursor.execute(f"SELECT TOP 1 ts FROM {table_name}")
            return (pmu_id, resolution)
        except Exception:
            return None
        finally:
            if cursor:
                with suppress(Exception):
                    cursor.close()
            if conn:
                self.connection_pool.return_connection(conn)

    def list_available_tables(  # noqa: PLR0912, PLR0915
        self,
        pmu_ids: Sequence[int] | None = None,
        resolutions: Sequence[int] | None = None,
        max_pmus: int | None = 10,
        parallel: bool = True,
        progress_callback: Callable[[int, int, int], None] | None = None,
    ) -> TableListResult:
        """
        List available PMU tables by checking existence of combinations.

        Args:
            pmu_ids: Optional list of PMU IDs to check
            resolutions: Optional list of resolutions to check
            max_pmus: Maximum number of PMUs to scan if pmu_ids not provided
            parallel: Whether to check tables in parallel (default True)
            progress_callback: Optional callback function called with (completed, total, found_count)

        Returns:
            TableListResult with found PMU/resolution combinations
        """
        from .signal_handler import get_cancellation_manager  # noqa: PLC0415

        pmus_to_scan = self.determine_pmus_to_scan(pmu_ids, max_pmus)
        if not pmus_to_scan:
            return TableListResult(found_pmus={})

        resolutions_to_scan = list(resolutions or self.DEFAULT_RESOLUTIONS)
        total_checks = len(pmus_to_scan) * len(resolutions_to_scan)

        self.logger.info(
            "Checking %s table combinations%s...",
            total_checks,
            " (parallel)" if parallel else " (sequential)",
        )

        if parallel:
            # Use parallel checking with ThreadPoolExecutor
            cancellation_manager = get_cancellation_manager()
            found: dict[int, list[int]] = {}
            found_count = 0
            last_progress_at = 0

            # Use connection pool size as max workers to avoid overwhelming the pool
            # Default to 3 if pool_size is not available or not an integer (e.g., in mocks/tests)
            pool_size = getattr(self.connection_pool, "pool_size", 3)
            if not isinstance(pool_size, int):
                pool_size = 3
            max_workers = min(pool_size, total_checks)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all table checks
                future_to_params = {}
                for pmu_id in pmus_to_scan:
                    for resolution in resolutions_to_scan:
                        future = executor.submit(self._check_single_table, pmu_id, resolution)
                        future_to_params[future] = (pmu_id, resolution)

                # Collect results as they complete

                for completed, future in enumerate(as_completed(future_to_params), start=1):
                    # Check for cancellation before processing each completed future
                    if cancellation_manager.is_cancelled():
                        self.logger.warning(
                            f"Table scan cancelled - {completed}/{total_checks} checks completed"
                        )
                        # Cancel remaining futures
                        for f in future_to_params:
                            if not f.done():
                                f.cancel()
                        break

                    result = future.result()

                    if result:
                        pmu_id, resolution = result
                        if pmu_id not in found:
                            found[pmu_id] = []
                        found[pmu_id].append(resolution)
                        found_count += 1

                    # Call progress callback every 5 tables OR every 10% (whichever comes first)
                    progress_interval = min(5, max(1, total_checks // 10))
                    should_report = (
                        completed - last_progress_at >= progress_interval
                        or completed == total_checks
                    )

                    if should_report and progress_callback:
                        progress_callback(completed, total_checks, found_count)
                        last_progress_at = completed

                    if completed % 10 == 0 or completed == total_checks:
                        self.logger.debug(
                            "Checked %d/%d table combinations", completed, total_checks
                        )
        else:
            # Fall back to sequential checking if parallel is disabled
            cancellation_manager = get_cancellation_manager()
            conn = self._acquire_connection()
            cursor = conn.cursor()
            found: dict[int, list[int]] = {}
            checked = 0
            found_count = 0
            last_progress_at = 0

            try:
                for pmu_id in pmus_to_scan:
                    for resolution in resolutions_to_scan:
                        # Check for cancellation before each table check
                        if cancellation_manager.is_cancelled():
                            self.logger.warning(
                                f"Table scan cancelled after {checked}/{total_checks} checks"
                            )
                            break

                        table_name = f"pmu_{pmu_id}_{resolution}"
                        checked += 1
                        with suppress(Exception):
                            cursor.execute(f"SELECT TOP 1 ts FROM {table_name}")
                            if pmu_id not in found:
                                found[pmu_id] = []
                            found[pmu_id].append(resolution)
                            found_count += 1

                        # Call progress callback every 5 tables OR every 10% (whichever comes first)
                        progress_interval = min(5, max(1, total_checks // 10))
                        should_report = (
                            checked - last_progress_at >= progress_interval
                            or checked == total_checks
                        )

                        if should_report and progress_callback:
                            progress_callback(checked, total_checks, found_count)
                            last_progress_at = checked

                    # Check again at the outer loop to break both loops
                    if cancellation_manager.is_cancelled():
                        break
            finally:
                self.connection_pool.return_connection(conn)

        sorted_found = {pmu_id: sorted(res_list) for pmu_id, res_list in found.items()}
        return TableListResult(found_pmus=sorted_found)

    # ------------------------------------------------------------- Table Access
    def test_table_access(self, table_name: str) -> bool:
        try:
            conn = self._acquire_connection()
        except TableManagerError as exc:
            self.logger.error(str(exc))
            return False

        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT TOP 1 ts FROM {table_name}")
            cursor.fetchone()
            # Clear any remaining result sets
            with suppress(Exception):
                while cursor.nextset():
                    pass
            return True
        except Exception as exc:
            self.logger.error("Table %s does not exist or is not accessible", table_name)
            self.logger.debug("Table access failure detail: %s", exc, exc_info=True)
            return False
        finally:
            # Close cursor and clear connection state
            if cursor:
                with suppress(Exception):
                    cursor.close()
            # Commit to clear any pending transaction state for custom JDBC
            with suppress(Exception):
                conn.commit()
            self.connection_pool.return_connection(conn)

    # ------------------------------------------------------------ Table Details
    def get_table_statistics(self, table_name: str) -> TableStatistics:
        """
        Get table statistics without using aggregate functions.

        Note: Custom JDBC implementation doesn't support COUNT, MIN, MAX, AVG, SUM.
        This method queries the table multiple times sequentially to gather stats.
        These queries cannot be parallelized as they must use the same connection
        context, and batching them into a single query is not possible due to
        JDBC limitations (no UNION support in result sets).

        See docs/phasor-point-sql/README.md for more on PhasorPoint SQL constraints.
        """
        conn = self._acquire_connection()

        # Clear any pending state on the connection for custom JDBC
        with suppress(Exception):
            conn.commit()

        cursor = conn.cursor()

        try:
            # Get column count using TOP 0 (this works)
            cursor.execute(f"SELECT TOP 0 * FROM {table_name}")
            column_count = len(cursor.description or [])
            with suppress(Exception):
                while cursor.nextset():
                    pass
            cursor.close()

            # For time range, query first and last rows
            # Assuming ts column exists and table is ordered by ts
            start_time = None
            end_time = None

            # Try to get first timestamp
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT TOP 1 ts FROM {table_name}")
                row = cursor.fetchone()
                if row and row[0] is not None:
                    start_time = row[0]
            except Exception as exc:
                self.logger.debug("Could not get start time: %s", exc)
            finally:
                with suppress(Exception):
                    while cursor.nextset():
                        pass
                cursor.close()

            # Try to get last timestamp using ORDER BY DESC
            cursor = conn.cursor()
            try:
                # Try ORDER BY ts DESC first
                cursor.execute(f"SELECT TOP 1 ts FROM {table_name} ORDER BY ts DESC")
                row = cursor.fetchone()
                if row and row[0] is not None:
                    end_time = row[0]
            except Exception:
                # If ORDER BY doesn't work, we'll have to sample data later
                self.logger.debug("ORDER BY not supported, time range will be incomplete")
            finally:
                with suppress(Exception):
                    while cursor.nextset():
                        pass
                cursor.close()

            # Row count is not available without COUNT - return 0 as placeholder
            # The actual row count will be shown as "Unknown" in the display
            row_count = 0

            return TableStatistics(
                row_count=row_count,
                column_count=int(column_count),
                start_time=start_time,
                end_time=end_time,
                bytes_estimate=None,
            )
        finally:
            with suppress(Exception):
                conn.commit()
            self.connection_pool.return_connection(conn)

    def get_sample_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        conn = self._acquire_connection()
        try:
            with suppress(Exception):
                return pd.read_sql(f"SELECT * FROM {table_name} LIMIT {limit}", conn)
            with suppress(Exception):
                return pd.read_sql(f"SELECT TOP {limit} * FROM {table_name}", conn)
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            return df.head(limit)
        finally:
            self.connection_pool.return_connection(conn)

    def get_table_info(
        self,
        pmu_id: int,
        resolution: int,
        *,
        sample_limit: int = 5,
    ) -> TableInfo | None:
        table_name = f"pmu_{pmu_id}_{resolution}"

        if not self.test_table_access(table_name):
            return None

        stats = self.get_table_statistics(table_name)
        sample = None
        with suppress(Exception):
            sample = self.get_sample_data(table_name, sample_limit)

        pmu_dataclass = self._config_manager.get_pmu_info(pmu_id)

        return TableInfo(
            pmu_id=pmu_id,
            resolution=resolution,
            table_name=table_name,
            statistics=stats,
            pmu_info=pmu_dataclass,
            sample_data=sample,
        )
