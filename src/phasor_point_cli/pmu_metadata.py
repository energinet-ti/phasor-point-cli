"""
PMU metadata retrieval from database.

This module provides functionality to dynamically fetch PMU metadata (IDs and station
names) from the pmu_data_statistics table and merge it with existing configuration data.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .connection_pool import JDBCConnectionPool

__all__ = ["fetch_pmu_metadata_from_database", "merge_pmu_metadata"]


def fetch_pmu_metadata_from_database(
    connection_pool: JDBCConnectionPool, logger: logging.Logger | None = None
) -> list[dict[str, Any]]:
    """
    Fetch PMU metadata (IDs and station names) from the pmu_data_statistics table.

    Uses the connection pool to obtain a database connection and query the
    pmu_data_statistics table for all unique PMU IDs and their corresponding station
    names. This function is used during setup to dynamically populate the configuration
    with available PMU metadata.

    Args:
        connection_pool: JDBCConnectionPool instance for obtaining database connections
        logger: Optional logger instance

    Returns:
        List of PMU dictionaries with 'id' and 'station_name' keys, sorted by ID

    Raises:
        Exception: If database connection or query fails
    """
    log = logger or logging.getLogger("pmu_metadata")

    pmus = []
    conn = connection_pool.get_connection()

    if not conn:
        log.error("Could not obtain database connection from pool")
        raise RuntimeError("Failed to obtain database connection")

    try:
        log.info("Fetching PMU list from database...")
        cursor = conn.cursor()

        # Query pmu_data_statistics for distinct PMU IDs and names
        # This table is documented in docs/phasor-point-sql/pmu_data_statistics.md
        query = """
            SELECT DISTINCT id, station_name
            FROM pmu_data_statistics
            ORDER BY id
        """

        log.debug(f"Executing query: {query}")
        cursor.execute(query)

        rows = cursor.fetchall()
        log.info(f"Found {len(rows)} PMUs in database")

        for row in rows:
            pmu_id = int(row.id) if row.id is not None else None
            station_name = str(row.station_name).strip() if row.station_name else f"PMU {pmu_id}"

            if pmu_id is not None:
                pmus.append({"id": pmu_id, "station_name": station_name})

        cursor.close()
        log.info(f"Successfully fetched {len(pmus)} PMUs from database")

    except Exception as exc:
        log.error(f"Failed to fetch PMUs from database: {exc}")
        raise
    finally:
        connection_pool.return_connection(conn)

    return pmus


def merge_pmu_metadata(
    existing_pmus: list[dict[str, Any]], new_pmus: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """
    Merge new PMU metadata with existing configuration.

    Strategy:
    - Use PMU ID as unique key
    - Update station name if ID exists with different name
    - Add new PMUs that don't exist in current config
    - Preserve any additional fields in existing PMU entries
    - Sort result by PMU ID

    Args:
        existing_pmus: Current PMU list from configuration
        new_pmus: Newly fetched PMU list from database

    Returns:
        Merged list of PMU dictionaries, sorted by ID
    """
    # Build lookup of existing PMUs by ID
    pmu_lookup: dict[int, dict[str, Any]] = {}
    for pmu in existing_pmus:
        if "id" in pmu and pmu["id"] is not None:
            pmu_lookup[pmu["id"]] = pmu.copy()

    # Process new PMUs
    for new_pmu in new_pmus:
        pmu_id = new_pmu.get("id")
        if pmu_id is None:
            continue

        if pmu_id in pmu_lookup:
            # Update existing PMU - update station_name and any other new fields
            pmu_lookup[pmu_id]["station_name"] = new_pmu["station_name"]
            # Add any other fields from new_pmu that don't exist
            for key, value in new_pmu.items():
                if key not in pmu_lookup[pmu_id]:
                    pmu_lookup[pmu_id][key] = value
        else:
            # Add new PMU
            pmu_lookup[pmu_id] = new_pmu.copy()

    # Convert back to sorted list (all entries have valid ID at this point)
    return sorted(pmu_lookup.values(), key=lambda x: x["id"])
