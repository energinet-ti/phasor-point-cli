"""
Table management services for the refactored PhasorPoint CLI.

The :class:`TableManager` encapsulates PMU table discovery, metadata retrieval,
and sampling logic. Legacy helpers in ``table_operations`` delegate to this
class to preserve backwards compatibility with existing command handlers while
the CLI migrates to the new OOP façade.
"""

from __future__ import annotations

import logging
from contextlib import suppress
from typing import Sequence

import pandas as pd

from .models import PMUInfo, TableInfo, TableListResult, TableStatistics


class TableManagerError(Exception):
    """Raised when a table operation cannot be completed."""


class TableManager:
    """Manage discovery and inspection of PMU tables."""

    DEFAULT_RESOLUTIONS: tuple[int, ...] = (1, 50, 200)

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
        available = config.get("available_pmus", {}) if config else {}
        for region, pmus in available.items():
            for entry in pmus:
                info = PMUInfo(
                    id=int(entry["id"]),
                    station_name=entry.get("station_name", "Unknown"),
                    region=region,
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
        available = config.get("available_pmus")
        if not available:
            self.logger.warning("No PMUs available in configuration. Provide explicit PMU IDs.")
            return None

        all_ids: list[int] = []
        for pmus in available.values():
            all_ids.extend(int(p["id"]) for p in pmus)

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
    def list_available_tables(
        self,
        pmu_ids: Sequence[int] | None = None,
        resolutions: Sequence[int] | None = None,
        max_pmus: int | None = 10,
    ) -> TableListResult:
        pmus_to_scan = self.determine_pmus_to_scan(pmu_ids, max_pmus)
        if not pmus_to_scan:
            return TableListResult(found_pmus={})

        resolutions_to_scan = list(resolutions or self.DEFAULT_RESOLUTIONS)
        conn = self._acquire_connection()
        cursor = conn.cursor()

        found: dict[int, list[int]] = {}
        total_checks = len(pmus_to_scan) * len(resolutions_to_scan)
        checked = 0

        try:
            self.logger.info("Checking %s table combinations...", total_checks)
            for pmu_id in pmus_to_scan:
                for res in resolutions_to_scan:
                    table_name = f"pmu_{pmu_id}_{res}"
                    checked += 1
                    with suppress(Exception):
                        cursor.execute(f"SELECT TOP 1 ts FROM {table_name}")
                        if pmu_id not in found:
                            found[pmu_id] = []
                        found[pmu_id].append(res)
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
        Custom JDBC implementation doesn't support COUNT, MIN, MAX.
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
