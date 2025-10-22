"""
Unit tests for the ConfigurationManager class.
"""

from __future__ import annotations

import json

import pytest

from phasor_point_cli.config import ConfigurationManager


def test_configuration_manager_uses_embedded_defaults():
    # Arrange
    manager = ConfigurationManager()

    # Act
    database = manager.get_database_config()
    extraction = manager.get_extraction_config()

    # Assert
    assert database["driver"] == "Psymetrix PhasorPoint"
    assert extraction["default_resolution"] == 1
    # Embedded defaults now have empty PMU list (populated dynamically during setup)
    assert len(manager.get_all_pmu_ids()) == 0


def test_configuration_manager_loads_from_file(tmp_path):
    # Arrange
    config_file = tmp_path / "config.json"
    payload = {
        "database": {"driver": "Custom Driver"},
        "extraction": {"default_resolution": 5},
        "data_quality": {
            "frequency_min": 49,
            "frequency_max": 51,
            "null_threshold_percent": 20,
            "gap_multiplier": 2,
        },
        "output": {"default_output_dir": "data"},
        "available_pmus": {"region": [{"id": 45012, "station_name": "Test PMU", "country": "FI"}]},
    }
    config_file.write_text(json.dumps(payload), encoding="utf-8")

    # Act
    manager = ConfigurationManager(config_file=str(config_file))

    # Assert
    assert manager.get_database_config()["driver"] == "Custom Driver"
    assert manager.get_pmu_info(45012).station_name == "Test PMU"


def test_get_pmu_info_handles_unknown_number():
    # Arrange
    manager = ConfigurationManager()

    # Act
    result = manager.get_pmu_info(99999)

    # Assert
    assert result is None


def test_data_quality_thresholds_return_dataclass():
    # Arrange
    manager = ConfigurationManager()

    # Act
    thresholds = manager.get_data_quality_thresholds()

    # Assert
    assert thresholds.frequency_min == 45
    assert thresholds.frequency_max == 65


def test_validate_raises_for_missing_sections():
    # Arrange
    manager = ConfigurationManager(config_data={"database": {}, "extraction": {}, "output": {}})

    # Act & Assert
    with pytest.raises(ValueError):
        manager.validate()


def test_validate_passes_for_complete_config():
    # Arrange
    manager = ConfigurationManager()

    # Act & Assert - should not raise
    manager.validate()


def test_get_all_pmu_ids_returns_sorted_list():
    # Arrange
    manager = ConfigurationManager(
        config_data={
            "database": {},
            "extraction": {},
            "data_quality": {
                "frequency_min": 40,
                "frequency_max": 60,
                "null_threshold_percent": 10,
                "gap_multiplier": 2,
            },
            "output": {},
            "available_pmus": {
                "region": [
                    {"id": 45014, "station_name": "PMU B"},
                    {"id": 45012, "station_name": "PMU A"},
                ]
            },
        }
    )

    # Act
    pmu_ids = manager.get_all_pmu_ids()

    # Assert
    assert pmu_ids == [45012, 45014]


def test_setup_configuration_files_creates_files(tmp_path, monkeypatch):
    # Arrange - Change working directory to tmp_path for local setup
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "config.json"
    env_path = tmp_path / ".env"

    # Act - Use local=True to create files in current directory (tmp_path)
    ConfigurationManager.setup_configuration_files(local=True, force=True)

    # Assert
    assert config_path.exists()
    assert env_path.exists()
    data = json.loads(config_path.read_text(encoding="utf-8"))
    assert "database" in data
    # Config should have empty PMU list initially (no database connection during test)
    assert data["available_pmus"]["all"] == []


def test_pmu_metadata_merge():
    """Test PMU metadata merging logic."""
    from phasor_point_cli.pmu_metadata import merge_pmu_metadata

    # Arrange
    existing = [
        {"id": 501, "station_name": "Old Name", "custom_field": "preserved"},
        {"id": 901, "station_name": "KEMINMAA"},
    ]
    new_pmus = [
        {"id": 501, "station_name": "New Name"},  # Update existing
        {"id": 1026, "station_name": "VHA400-P1"},  # Add new
    ]

    # Act
    merged = merge_pmu_metadata(existing, new_pmus)

    # Assert
    assert len(merged) == 3
    # Check updated PMU keeps custom fields and gets new station_name
    pmu_501 = next(p for p in merged if p["id"] == 501)
    assert pmu_501["station_name"] == "New Name"
    assert pmu_501["custom_field"] == "preserved"
    # Check existing unchanged PMU
    assert any(p["id"] == 901 and p["station_name"] == "KEMINMAA" for p in merged)
    # Check new PMU added
    assert any(p["id"] == 1026 and p["station_name"] == "VHA400-P1" for p in merged)
    # Check sorting by ID
    assert merged[0]["id"] < merged[1]["id"] < merged[2]["id"]
