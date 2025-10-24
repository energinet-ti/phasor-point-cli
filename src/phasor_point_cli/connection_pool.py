"""
Database connection pool management for PhasorPoint CLI
"""

import logging
import threading


class JDBCConnectionPool:
    """Simple connection pool for JDBC endpoints"""

    def __init__(self, connection_string, max_connections=3, logger=None):
        self.connection_string = connection_string
        self.max_connections = max_connections
        self.pool = []
        self.lock = threading.Lock()
        self.logger = logger or logging.getLogger("phasor_cli")

    @property
    def pool_size(self):
        """Configured maximum number of pooled connections."""
        return self.max_connections

    @property
    def available_connections(self):
        """Number of idle connections currently available in the pool."""
        with self.lock:
            return len(self.pool)

    def get_connection(self):
        """Get a connection from the pool or create new one"""
        with self.lock:
            if self.pool:
                conn = self.pool.pop()
                self.logger.debug(f"Reused connection from pool (pool size: {len(self.pool)})")
                return conn

            # Create new connection if pool is empty and under limit
            if len(self.pool) < self.max_connections:
                try:
                    # Lazy import pyodbc to avoid loading native dependencies at module import time
                    import pyodbc  # noqa: PLC0415 - late import  # pragma: no cover

                    conn = pyodbc.connect(self.connection_string)
                    self.logger.debug(f"Created new connection (pool size: {len(self.pool)})")
                    return conn
                except Exception as e:
                    self.logger.error(f"Failed to create connection: {e}")
                    return None

            # Pool exhausted, return None
            self.logger.warning(f"Connection pool exhausted (max: {self.max_connections})")
            return None

    def return_connection(self, conn):
        """Return connection to pool"""
        if conn and len(self.pool) < self.max_connections:
            with self.lock:
                self.pool.append(conn)
                self.logger.debug(f"Returned connection to pool (pool size: {len(self.pool)})")
        else:
            # Pool full or connection invalid, close it
            try:
                if conn:
                    conn.close()
                    self.logger.debug("Closed connection (pool full or invalid)")
            except Exception as e:
                self.logger.debug(f"Error closing connection: {e}")

    def cleanup(self):
        """Close all pooled connections"""
        with self.lock:
            connection_count = len(self.pool)
            for conn in self.pool:
                try:
                    conn.close()
                except Exception as e:
                    self.logger.debug(f"Error closing pooled connection: {e}")
            self.pool.clear()
            self.logger.info(f"Closed {connection_count} pooled connections")

    def resize(self, new_size):
        """Resize the pool, closing surplus idle connections when shrinking."""
        if new_size <= 0:
            raise ValueError("new_size must be positive")

        with self.lock:
            if new_size == self.max_connections:
                return

            if new_size < self.max_connections:
                while len(self.pool) > new_size:
                    conn = self.pool.pop()
                    try:
                        conn.close()
                    except Exception as e:
                        self.logger.debug(f"Error closing connection during resize: {e}")

            self.max_connections = new_size
            self.logger.info(f"Resized connection pool to {new_size}")
