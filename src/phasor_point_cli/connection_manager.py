"""
Connection management utilities for the OOP PhasorPoint CLI refactor.

The ``ConnectionManager`` encapsulates credential discovery, validation, and
creation of database connection pools. It delegates configuration retrieval to
``ConfigurationManager`` and keeps the CLI facade free from low level
connection concerns.
"""

from __future__ import annotations

import os

from .config import ConfigurationManager
from .connection_pool import JDBCConnectionPool


class ConnectionManager:
    """Manage database credentials and connection pool creation."""

    def __init__(self, config_manager: ConfigurationManager, logger):
        self.config_manager = config_manager
        self.logger = logger

        self.username: str | None = None
        self.password: str | None = None
        self.host: str | None = None
        self.port: int | None = None
        self.database: str | None = None

        database_cfg = self.config_manager.get_database_config()
        self.driver = database_cfg.get("driver", "Psymetrix PhasorPoint")

    # ------------------------------------------------------------------ Set up
    def setup_credentials(self, username: str | None = None, password: str | None = None) -> None:
        """
        Populate credentials from CLI arguments or environment variables.

        CLI supplied credentials take precedence over environment variables. All
        remaining connection parameters are sourced from the environment, which
        mirrors the behaviour of the legacy CLI implementation.
        """
        env = os.environ

        self.username = username or env.get("DB_USERNAME")
        self.password = password or env.get("DB_PASSWORD")
        self.host = env.get("DB_HOST")

        port_value = env.get("DB_PORT")
        self.port = int(port_value) if port_value and port_value.isdigit() else None

        self.database = env.get("DB_NAME")

    def validate_credentials(self, *, raise_errors: bool = True) -> bool:
        """Ensure all required credential fields are populated."""
        missing = []
        if not self.username:
            missing.append("DB_USERNAME")
        if not self.password:
            missing.append("DB_PASSWORD")
        if not self.host:
            missing.append("DB_HOST")
        if not self.port:
            missing.append("DB_PORT")
        if not self.database:
            missing.append("DB_NAME")

        if missing:
            message = f"Missing connection configuration: {', '.join(missing)}"
            if raise_errors:
                raise ValueError(message)
            self.logger.error(message)
            return False
        return True

    def build_connection_string(self) -> str:
        """Construct a DSN style connection string."""
        self.validate_credentials()
        return (
            f"DRIVER={{{self.driver}}};"
            f"HOST={self.host};"
            f"PORT={self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password}"
        )

    def create_connection_pool(self, pool_size: int = 1) -> JDBCConnectionPool:
        """Initialise a JDBCConnectionPool using the configured credentials."""
        if pool_size <= 0:
            raise ValueError("pool_size must be positive")

        connection_string = self.build_connection_string()
        return JDBCConnectionPool(connection_string, max_connections=pool_size, logger=self.logger)

    # ------------------------------------------------------------------ Helpers
    @property
    def is_configured(self) -> bool:
        """Boolean flag indicating whether credentials are available."""
        return self.validate_credentials(raise_errors=False)
