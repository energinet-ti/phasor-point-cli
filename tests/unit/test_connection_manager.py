"""
Unit tests for the ConnectionManager class.
"""

from __future__ import annotations

import logging

import pytest

from phasor_point_cli.config import ConfigurationManager
from phasor_point_cli.connection_manager import ConnectionManager


@pytest.fixture
def config_manager():
    return ConfigurationManager(
        config_data={
            "database": {"driver": "Test Driver"},
            "extraction": {},
            "data_quality": {
                "frequency_min": 40,
                "frequency_max": 60,
                "null_threshold_percent": 10,
                "gap_multiplier": 2,
            },
            "output": {},
            "available_pmus": [{"number": 1, "name": "Test"}],
        }
    )


@pytest.fixture
def logger():
    return logging.getLogger("test_connection_manager")


def test_setup_credentials_prefers_cli_arguments(config_manager, logger, monkeypatch):
    # Arrange
    monkeypatch.setenv("DB_USERNAME", "env_user")
    monkeypatch.setenv("DB_PASSWORD", "env_pass")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "phasor")
    manager = ConnectionManager(config_manager, logger)

    # Act
    manager.setup_credentials(username="cli_user", password="cli_pass")

    # Assert
    assert manager.username == "cli_user"
    assert manager.password == "cli_pass"


def test_setup_credentials_uses_environment_variables(config_manager, logger, monkeypatch):
    # Arrange
    monkeypatch.setenv("DB_USERNAME", "env_user")
    monkeypatch.setenv("DB_PASSWORD", "env_pass")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "phasor")
    manager = ConnectionManager(config_manager, logger)

    # Act
    manager.setup_credentials()

    # Assert
    assert manager.username == "env_user"
    assert manager.port == 5432


def test_validate_credentials_success(config_manager, logger, monkeypatch):
    # Arrange
    monkeypatch.setenv("DB_USERNAME", "env_user")
    monkeypatch.setenv("DB_PASSWORD", "env_pass")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "phasor")
    manager = ConnectionManager(config_manager, logger)
    manager.setup_credentials()

    # Act
    result = manager.validate_credentials()

    # Assert
    assert result is True


def test_validate_credentials_missing(config_manager, logger):
    # Arrange
    manager = ConnectionManager(config_manager, logger)

    # Act & Assert
    with pytest.raises(ValueError):
        manager.validate_credentials()


def test_build_connection_string(config_manager, logger, monkeypatch):
    # Arrange
    monkeypatch.setenv("DB_USERNAME", "env_user")
    monkeypatch.setenv("DB_PASSWORD", "env_pass")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "phasor")
    manager = ConnectionManager(config_manager, logger)
    manager.setup_credentials()

    # Act
    connection_string = manager.build_connection_string()

    # Assert
    assert "UID=env_user" in connection_string
    assert "PWD=env_pass" in connection_string
    assert "DRIVER={Test Driver}" in connection_string


def test_create_connection_pool(config_manager, logger, monkeypatch, mocker):
    # Arrange
    monkeypatch.setenv("DB_USERNAME", "env_user")
    monkeypatch.setenv("DB_PASSWORD", "env_pass")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "phasor")
    manager = ConnectionManager(config_manager, logger)
    manager.setup_credentials()
    mocker.patch("pyodbc.connect")  # Prevent any real connection attempts

    # Act
    pool = manager.create_connection_pool(pool_size=3)

    # Assert
    assert pool.pool_size == 3
    assert pool.connection_string.startswith("DRIVER={Test Driver}")


def test_is_configured_property(config_manager, logger, monkeypatch):
    # Arrange
    manager = ConnectionManager(config_manager, logger)

    # Act & Assert - check initial state
    assert manager.is_configured is False

    # Arrange - setup environment
    monkeypatch.setenv("DB_USERNAME", "env_user")
    monkeypatch.setenv("DB_PASSWORD", "env_pass")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "phasor")

    # Act
    manager.setup_credentials()

    # Assert
    assert manager.is_configured is True
