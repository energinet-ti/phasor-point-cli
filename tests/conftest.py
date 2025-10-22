"""
Pytest configuration and shared fixtures for PhasorPoint CLI tests

This module provides common fixtures and test utilities that can be used
across all test files in the suite.
"""

import shutil
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

# Mock pyodbc module to avoid native library dependency issues on macOS/Windows during testing
# This must happen before any test imports phasor_point_cli modules
if "pyodbc" not in sys.modules:
    mock_pyodbc = MagicMock()
    mock_pyodbc.connect = MagicMock()
    sys.modules["pyodbc"] = mock_pyodbc


@pytest.fixture
def mock_db_connection(mocker):
    """
    Mock database connection that simulates pyodbc.connect()

    Usage:
        def test_something(mock_db_connection):
            # Connection is already mocked
            conn = pyodbc.connect(...)
    """
    mock_conn = MagicMock()

    def make_cursor():
        mock_cursor = MagicMock()
        mock_cursor.nextset.return_value = None  # No more result sets
        mock_cursor.close.return_value = None  # Allow cursor to be closed
        return mock_cursor

    # Return a new cursor each time cursor() is called
    mock_conn.cursor.side_effect = make_cursor

    # Mock the pyodbc.connect to return our mock connection
    mocker.patch("pyodbc.connect", return_value=mock_conn)

    return mock_conn


@pytest.fixture
def mock_env_vars(monkeypatch):
    """
    Set up mock environment variables for database credentials

    Usage:
        def test_something(mock_env_vars):
            # Environment variables are already set
    """
    monkeypatch.setenv("DB_HOST", "test_host")
    monkeypatch.setenv("DB_PORT", "1234")
    monkeypatch.setenv("DB_NAME", "test_db")
    monkeypatch.setenv("DB_USERNAME", "test_user")
    monkeypatch.setenv("DB_PASSWORD", "test_pass")

    return {
        "DB_HOST": "test_host",
        "DB_PORT": "1234",
        "DB_NAME": "test_db",
        "DB_USERNAME": "test_user",
        "DB_PASSWORD": "test_pass",
    }


@pytest.fixture
def sample_config():
    """
    Provide a sample configuration dictionary for testing

    Returns:
        dict: Sample configuration matching config.json structure
    """
    return {
        "database": {"driver": "Psymetrix PhasorPoint"},
        "extraction": {
            "default_resolution": 1,
            "default_clean": True,
            "timezone_handling": "machine_timezone",
        },
        "data_quality": {
            "frequency_min": 45,
            "frequency_max": 65,
            "null_threshold_percent": 50,
            "gap_multiplier": 5,
        },
        "output": {
            "default_output_dir": "data_exports",
            "timestamp_format": "%Y%m%d_%H%M%S",
            "timestamp_display_format": "%Y-%m-%d %H:%M:%S.%f",
            "compression": "snappy",
        },
        "available_pmus": {
            "test": [
                {"number": 45012, "name": "HKS_400 P1"},
                {"number": 45052, "name": "EDR400-P1"},
                {"number": 45022, "name": "AHA220-TRI"},
            ]
        },
    }


@pytest.fixture
def sample_pmu_dataframe():
    """
    Create a sample PMU dataframe with realistic data structure

    Returns:
        pd.DataFrame: Sample PMU data with timestamps, voltages, currents, and frequency
    """
    # Generate 100 samples at 50Hz (2 seconds of data)
    n_samples = 100
    start_time = datetime(2025, 1, 1, 12, 0, 0)

    timestamps = [start_time + timedelta(seconds=i * 0.02) for i in range(n_samples)]

    # Realistic PMU data values
    data = {
        "ts": timestamps,
        "va1_m": np.random.normal(230000, 1000, n_samples),  # Phase A voltage magnitude
        "vb1_m": np.random.normal(230000, 1000, n_samples),  # Phase B voltage magnitude
        "vc1_m": np.random.normal(230000, 1000, n_samples),  # Phase C voltage magnitude
        "va1_a": np.random.normal(0, 0.01, n_samples),  # Phase A voltage angle (radians)
        "vb1_a": np.random.normal(-2.094, 0.01, n_samples),  # Phase B voltage angle
        "vc1_a": np.random.normal(2.094, 0.01, n_samples),  # Phase C voltage angle
        "ia1_m": np.random.normal(500, 10, n_samples),  # Phase A current magnitude
        "ib1_m": np.random.normal(500, 10, n_samples),  # Phase B current magnitude
        "ic1_m": np.random.normal(500, 10, n_samples),  # Phase C current magnitude
        "ia1_a": np.random.normal(-0.5, 0.01, n_samples),  # Phase A current angle
        "ib1_a": np.random.normal(-2.594, 0.01, n_samples),  # Phase B current angle
        "ic1_a": np.random.normal(1.594, 0.01, n_samples),  # Phase C current angle
        "f": np.random.normal(50.0, 0.01, n_samples),  # Frequency
        "dfdt": np.random.normal(0, 0.001, n_samples),  # Rate of change of frequency
    }

    return pd.DataFrame(data)


@pytest.fixture
def sample_pmu_dataframe_with_nulls():
    """
    Create a sample PMU dataframe with null values for testing data validation

    Returns:
        pd.DataFrame: Sample PMU data with intentional null values
    """
    n_samples = 100
    start_time = datetime(2025, 1, 1, 12, 0, 0)

    timestamps = [start_time + timedelta(seconds=i * 0.02) for i in range(n_samples)]

    data = {
        "ts": timestamps,
        "va1_m": np.random.normal(230000, 1000, n_samples),
        "vb1_m": np.random.normal(230000, 1000, n_samples),
        "vc1_m": np.random.normal(230000, 1000, n_samples),
        "va1_a": np.random.normal(0, 0.01, n_samples),
        "vb1_a": np.random.normal(-2.094, 0.01, n_samples),
        "vc1_a": np.random.normal(2.094, 0.01, n_samples),
        "ia1_m": np.random.normal(500, 10, n_samples),
        "ib1_m": np.random.normal(500, 10, n_samples),
        "ic1_m": np.random.normal(500, 10, n_samples),
        "f": np.random.normal(50.0, 0.01, n_samples),
    }

    df = pd.DataFrame(data)

    # Introduce nulls in various columns
    df.loc[10:15, "va1_m"] = np.nan
    df.loc[20:25, "f"] = np.nan
    df.loc[50, "ia1_m"] = np.nan

    return df


@pytest.fixture
def temp_output_dir():
    """
    Create a temporary directory for test outputs

    Yields:
        Path: Path to temporary directory

    Cleanup:
        Automatically removes the directory after test completion
    """
    temp_dir = Path(tempfile.mkdtemp(prefix="phasor_test_"))
    yield temp_dir

    # Cleanup
    if temp_dir.exists():
        shutil.rmtree(temp_dir)


@pytest.fixture
def mock_logger(mocker):
    """
    Create a mock logger for testing logging behavior

    Returns:
        Mock: Mock logger instance
    """
    return mocker.MagicMock()


@pytest.fixture
def mock_cli_instance(mock_env_vars, sample_config, mock_db_connection, mock_logger, mocker):
    """
    Create a mocked PhasorPointCLI instance with all dependencies mocked

    This is a comprehensive fixture that sets up a fully functional mock CLI
    for testing without requiring actual database connections.

    Returns:
        PhasorPointCLI: CLI instance with mocked dependencies
    """
    import sys
    from pathlib import Path

    # Add src to path if not already there
    src_path = Path(__file__).parent.parent / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    from phasor_point_cli.cli import PhasorPointCLI

    # Mock the _load_config method to return sample config
    mocker.patch.object(PhasorPointCLI, "_load_config", return_value=sample_config)

    # Create CLI instance
    return PhasorPointCLI(
        username="test_user",
        password="test_pass",
        config_file=None,
        connection_pool_size=1,
        logger=mock_logger,
        skip_validation=False,
    )


@pytest.fixture
def fixed_time():
    """
    Provide a fixed datetime for testing time-dependent functions

    Returns:
        datetime: Fixed datetime (2025-01-15 10:00:00)
    """
    return datetime(2025, 1, 15, 10, 0, 0)


@pytest.fixture
def sample_extraction_log():
    """
    Provide a sample extraction log structure

    Returns:
        dict: Empty extraction log dictionary with proper structure
    """
    return {
        "extraction_info": {},
        "data_quality": {},
        "column_changes": {"removed": [], "renamed": [], "added": [], "type_conversions": []},
        "issues_found": [],
        "statistics": {},
    }
