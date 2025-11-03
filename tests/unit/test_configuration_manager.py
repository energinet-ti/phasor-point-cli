"""
Unit tests for the ConfigurationManager class.
"""

from __future__ import annotations

import json

import pytest

from phasor_point_cli.config import ConfigurationManager
from phasor_point_cli.constants import CLI_COMMAND_PYTHON


def test_configuration_manager_uses_embedded_defaults():
    # Arrange
    manager = ConfigurationManager()

    # Act
    database = manager.get_database_config()
    extraction = manager.get_extraction_config()

    # Assert
    assert database["driver"] == "Psymetrix PhasorPoint"
    assert extraction["default_resolution"] == 50
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
        "available_pmus": [{"id": 45012, "station_name": "Test PMU", "country": "FI"}],
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
    # Arrange - Now validation happens during init, so this test validates the old validate() method behavior
    manager = ConfigurationManager()  # Has all sections

    # Act & Assert - validate() still checks for missing sections if called explicitly
    manager.validate()  # Should not raise since embedded defaults are complete


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
            "available_pmus": [
                {"id": 45014, "station_name": "PMU B"},
                {"id": 45012, "station_name": "PMU A"},
            ],
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
    assert data["available_pmus"] == []


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


def test_config_invalid_json_exits_gracefully(tmp_path, capsys):
    """Test that invalid JSON config file exits with helpful error message."""
    # Arrange - Create config file with invalid JSON
    config_file = tmp_path / "config.json"
    config_file.write_text('{"database": {"driver": "test",}', encoding="utf-8")  # Trailing comma

    # Act & Assert - Should exit with code 1
    with pytest.raises(SystemExit) as exc_info:
        ConfigurationManager(config_file=str(config_file))

    assert exc_info.value.code == 1

    # Check error message is helpful
    captured = capsys.readouterr()
    assert "Invalid JSON format" in captured.out
    assert "config file" in captured.out.lower()
    assert f"{CLI_COMMAND_PYTHON} setup --force" in captured.out


def test_config_missing_file_uses_defaults():
    """Test that missing config file falls back to embedded defaults."""
    # Arrange - Use non-existent file path
    nonexistent = "/tmp/nonexistent_config_file_xyz.json"

    # Act
    manager = ConfigurationManager(config_file=nonexistent)

    # Assert - Should use embedded defaults
    assert manager.get_database_config()["driver"] == "Psymetrix PhasorPoint"
    assert manager.get_extraction_config()["default_resolution"] == 50


def test_config_file_directory_path_exits_gracefully(tmp_path):
    """Test that config file read errors (like passing a directory) exit gracefully."""
    # Arrange - Use a directory path instead of file (will cause read error)
    fake_config_dir = tmp_path / "not_a_file"
    fake_config_dir.mkdir()

    # Act & Assert - Should exit with SystemExit
    with pytest.raises(SystemExit) as exc_info:
        ConfigurationManager(config_file=str(fake_config_dir))

    assert exc_info.value.code == 1


# ============================================================================
# PMU Lookup Error Handling Tests (HIGH PRIORITY - Critical Paths)
# ============================================================================


class TestPMULookupErrorHandling:
    """Test PMU lookup error handling for malformed data."""

    def test_pmu_entry_missing_id_field(self, capsys):
        """Test that PMU entry missing 'id' field is skipped with warning."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {},
            "output": {},
            "available_pmus": [
                {"station_name": "Missing ID PMU"},  # Missing 'id' field
                {"id": 45012, "station_name": "Valid PMU"},  # Valid entry
            ],
        }

        # Act
        manager = ConfigurationManager(config_data=config_data)

        # Assert
        assert 45012 in manager.get_all_pmu_ids()  # Valid PMU loaded
        assert len(manager.get_all_pmu_ids()) == 1  # Only one PMU loaded

        # Check warning message
        captured = capsys.readouterr()
        assert "Issues found in PMU configuration" in captured.err
        assert "malformed PMU entries" in captured.err

    def test_pmu_entry_invalid_type(self, capsys):
        """Test that PMU entry with wrong type (string instead of dict) is skipped."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {},
            "output": {},
            "available_pmus": [
                "not a dictionary",  # Wrong type
                {"id": 45012, "station_name": "Valid PMU"},
            ],
        }

        # Act
        manager = ConfigurationManager(config_data=config_data)

        # Assert
        assert 45012 in manager.get_all_pmu_ids()
        assert len(manager.get_all_pmu_ids()) == 1

        captured = capsys.readouterr()
        assert "malformed PMU entries" in captured.err

    def test_pmu_entry_invalid_id_value(self, capsys):
        """Test that PMU entry with non-numeric ID is skipped."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {},
            "output": {},
            "available_pmus": [
                {"id": "not_a_number", "station_name": "Invalid ID"},
                {"id": 45012, "station_name": "Valid PMU"},
            ],
        }

        # Act
        manager = ConfigurationManager(config_data=config_data)

        # Assert
        assert 45012 in manager.get_all_pmu_ids()
        assert len(manager.get_all_pmu_ids()) == 1

        captured = capsys.readouterr()
        assert "malformed PMU entries" in captured.err

    def test_non_iterable_region_entries(self, capsys):
        """Test that available_pmus being a non-list is handled gracefully."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {},
            "output": {},
            "available_pmus": 12345,  # Not a list
        }

        # Act
        manager = ConfigurationManager(config_data=config_data)

        # Assert
        assert len(manager.get_all_pmu_ids()) == 0

    def test_string_instead_of_list_for_region(self, capsys):
        """Test that string value instead of list for available_pmus is handled."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {},
            "output": {},
            "available_pmus": "should be a list",  # String instead of list
        }

        # Act
        manager = ConfigurationManager(config_data=config_data)

        # Assert
        assert len(manager.get_all_pmu_ids()) == 0

    def test_mixed_valid_and_invalid_pmu_entries(self, capsys):
        """Test that valid PMUs are loaded despite some malformed entries."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {},
            "output": {},
            "available_pmus": [
                {"id": 45012, "station_name": "PMU 1"},  # Valid
                {"station_name": "Missing ID"},  # Invalid - missing id
                {"id": 45013, "station_name": "PMU 2"},  # Valid
                "not a dict",  # Invalid - wrong type
                {"id": 45014, "station_name": "PMU 3"},  # Valid
            ],
        }

        # Act
        manager = ConfigurationManager(config_data=config_data)

        # Assert
        assert len(manager.get_all_pmu_ids()) == 3
        assert 45012 in manager.get_all_pmu_ids()
        assert 45013 in manager.get_all_pmu_ids()
        assert 45014 in manager.get_all_pmu_ids()

        captured = capsys.readouterr()
        assert "2 malformed PMU entries" in captured.err
        assert "Successfully loaded 3 valid PMU(s)" in captured.err

    def test_duplicate_pmu_ids(self, capsys):
        """Test that duplicate PMU IDs result in last entry winning with warning."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {},
            "output": {},
            "available_pmus": [
                {"id": 45012, "station_name": "First PMU"},
                {"id": 45012, "station_name": "Second PMU"},  # Duplicate
                {"id": 45012, "station_name": "Third PMU"},  # Duplicate
            ],
        }

        # Act
        manager = ConfigurationManager(config_data=config_data)

        # Assert
        assert len(manager.get_all_pmu_ids()) == 1
        assert manager.get_pmu_info(45012).station_name == "Third PMU"  # Last wins

        captured = capsys.readouterr()
        assert "duplicate PMU IDs" in captured.err
        # 3 total occurrences should report "appears 3 times"
        assert "PMU ID 45012" in captured.err

    def test_available_pmus_not_dict(self, capsys):
        """Test that available_pmus not being a list is handled gracefully."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {},
            "output": {},
            "available_pmus": "not a list",  # Wrong type
        }

        # Act
        manager = ConfigurationManager(config_data=config_data)

        # Assert
        assert len(manager.get_all_pmu_ids()) == 0
        # No crash, just empty PMU list


# ============================================================================
# Dotted Path Access Tests (MEDIUM PRIORITY - Public API)
# ============================================================================


class TestDottedPathAccess:
    """Test dotted notation access in get() method."""

    def test_get_with_simple_key(self):
        """Test get() with simple key (no dots)."""
        # Arrange
        manager = ConfigurationManager()

        # Act
        result = manager.get("database")

        # Assert
        assert result["driver"] == "Psymetrix PhasorPoint"

    def test_get_with_nested_key(self):
        """Test get() with dotted key for nested access."""
        # Arrange
        manager = ConfigurationManager()

        # Act
        result = manager.get("database.driver")

        # Assert
        assert result == "Psymetrix PhasorPoint"

    def test_get_with_deep_nesting(self):
        """Test get() with deeply nested key."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {},
            "output": {},
            "custom": {"level1": {"level2": {"level3": "deep_value"}}},
        }
        manager = ConfigurationManager(config_data=config_data)

        # Act
        result = manager.get("custom.level1.level2.level3")

        # Assert
        assert result == "deep_value"

    def test_get_missing_nested_key_returns_default(self):
        """Test get() returns default for missing nested key."""
        # Arrange
        manager = ConfigurationManager()

        # Act
        result = manager.get("database.missing.key", "default_value")

        # Assert
        assert result == "default_value"

    def test_get_non_dict_intermediate_returns_default(self):
        """Test get() returns default when intermediate value is not dict."""
        # Arrange
        manager = ConfigurationManager()

        # Act
        result = manager.get("database.driver.invalid", "default")

        # Assert
        assert result == "default"  # driver is string, can't access .invalid

    def test_get_returns_deep_copy(self):
        """Test that get() returns deep copy, not reference."""
        # Arrange
        manager = ConfigurationManager()

        # Act
        result1 = manager.get("database")
        result1["modified"] = True
        result2 = manager.get("database")

        # Assert
        assert "modified" in result1
        assert "modified" not in result2  # Original unchanged


# ============================================================================
# Validation Warning Tests (MEDIUM PRIORITY)
# ============================================================================


class TestValidationWarnings:
    """Test validation warning paths."""

    def test_validate_warns_when_no_pmus_configured(self, caplog):
        """Test validate() warns when PMU lookup is empty."""
        # Arrange
        import logging

        caplog.set_level(logging.WARNING)
        manager = ConfigurationManager()  # Embedded defaults have empty PMU list

        # Act
        manager.validate()

        # Assert
        assert any("does not define any available PMUs" in rec.message for rec in caplog.records)
        assert any("setup --refresh-pmus" in rec.message for rec in caplog.records)


# ============================================================================
# Data Quality Edge Cases (LOW-MEDIUM PRIORITY)
# ============================================================================


class TestDataQualityEdgeCases:
    """Test data quality configuration edge cases."""

    def test_data_quality_section_missing_uses_defaults(self):
        """Test that missing data_quality section uses defaults."""
        # Arrange
        config_data = {"database": {}, "extraction": {}, "data_quality": {}, "output": {}}
        manager = ConfigurationManager(config_data=config_data)

        # Act
        thresholds = manager.get_data_quality_thresholds()

        # Assert
        assert thresholds.frequency_min == 45
        assert thresholds.frequency_max == 65
        assert thresholds.null_threshold_percent == 50
        assert thresholds.gap_multiplier == 5

    def test_data_quality_null_value_fails_validation(self, capsys):
        """Test that data_quality: null fails validation (must be dict)."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": None,  # Explicitly null - invalid
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "data_quality" in captured.out
        assert "must be a dictionary" in captured.out


# ============================================================================
# Tier 1 Validation Tests (Structure and Types)
# ============================================================================


class TestTier1Validation:
    """Test Tier 1 validation - structure and types."""

    def test_missing_database_section_fails(self, capsys):
        """Test that missing database section fails validation."""
        # Arrange
        config_data = {"extraction": {}, "data_quality": {}, "output": {}}

        # Act & Assert
        with pytest.raises(SystemExit) as exc_info:
            ConfigurationManager(config_data=config_data)

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Missing required configuration section: 'database'" in captured.out
        assert f"{CLI_COMMAND_PYTHON} setup --force" in captured.out

    def test_missing_extraction_section_fails(self, capsys):
        """Test that missing extraction section fails validation."""
        # Arrange
        config_data = {"database": {}, "data_quality": {}, "output": {}}

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "Missing required configuration section: 'extraction'" in captured.out

    def test_section_not_dict_fails(self, capsys):
        """Test that section being non-dict fails validation."""
        # Arrange
        config_data = {
            "database": "not a dict",
            "extraction": {},
            "data_quality": {},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "must be a dictionary" in captured.out

    def test_database_driver_wrong_type_fails(self, capsys):
        """Test that database.driver with wrong type fails validation."""
        # Arrange
        config_data = {
            "database": {"driver": 12345},  # Should be string
            "extraction": {},
            "data_quality": {},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "database.driver must be a string" in captured.out
        assert "Found: int" in captured.out

    def test_resolution_wrong_type_fails(self, capsys):
        """Test that resolution with wrong type fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {"default_resolution": "50"},  # Should be int
            "data_quality": {},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "default_resolution must be an integer" in captured.out

    def test_default_clean_wrong_type_fails(self, capsys):
        """Test that default_clean with wrong type fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {"default_clean": "yes"},  # Should be bool
            "data_quality": {},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "default_clean must be a boolean" in captured.out

    def test_invalid_timezone_handling_value_fails(self, capsys):
        """Test that invalid timezone_handling enum value fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {"timezone_handling": "invalid_value"},
            "data_quality": {},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "timezone_handling has invalid value" in captured.out
        assert "machine_timezone, utc, local" in captured.out

    def test_data_quality_field_wrong_type_fails(self, capsys):
        """Test that data_quality fields with wrong types fail validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {"frequency_min": "forty-five"},  # Should be number
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "frequency_min must be a number" in captured.out

    def test_empty_output_dir_fails(self, capsys):
        """Test that empty default_output_dir fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {},
            "output": {"default_output_dir": "   "},  # Empty/whitespace
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "default_output_dir cannot be empty" in captured.out

    def test_invalid_compression_value_fails(self, capsys):
        """Test that invalid compression enum value fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {},
            "output": {"compression": "zip"},  # Invalid, should be snappy/gzip/none
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "compression has invalid value" in captured.out
        assert "snappy, gzip, none" in captured.out


# ============================================================================
# Tier 2 Validation Tests (Logical Constraints)
# ============================================================================


class TestTier2Validation:
    """Test Tier 2 validation - logical constraints."""

    def test_negative_resolution_fails(self, capsys):
        """Test that negative resolution fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {"default_resolution": -5},
            "data_quality": {},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "must be positive" in captured.out

    def test_zero_resolution_fails(self, capsys):
        """Test that zero resolution fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {"default_resolution": 0},
            "data_quality": {},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "must be positive" in captured.out

    def test_very_high_resolution_warns(self, caplog):
        """Test that unusually high resolution generates warning."""
        # Arrange
        import logging

        caplog.set_level(logging.WARNING)
        config_data = {
            "database": {},
            "extraction": {"default_resolution": 5000},  # > 1000
            "data_quality": {},
            "output": {},
        }

        # Act
        ConfigurationManager(config_data=config_data)

        # Assert
        assert any("unusually high" in rec.message for rec in caplog.records)

    def test_frequency_max_less_than_min_fails(self, capsys):
        """Test that frequency_max <= frequency_min fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {"frequency_min": 60, "frequency_max": 50},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "frequency_max (50) must be greater than frequency_min (60)" in captured.out

    def test_frequency_min_out_of_range_fails(self, capsys):
        """Test that frequency_min outside 0-100 fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {"frequency_min": 150},  # > 100
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "frequency_min (150) must be between 0 and 100" in captured.out

    def test_frequency_max_out_of_range_fails(self, capsys):
        """Test that frequency_max outside 0-100 fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {"frequency_max": -10},  # < 0
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "frequency_max (-10) must be between 0 and 100" in captured.out

    def test_null_threshold_over_100_fails(self, capsys):
        """Test that null_threshold_percent > 100 fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {"null_threshold_percent": 150},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "null_threshold_percent (150) must be between 0 and 100" in captured.out

    def test_null_threshold_negative_fails(self, capsys):
        """Test that null_threshold_percent < 0 fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {"null_threshold_percent": -5},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "null_threshold_percent (-5) must be between 0 and 100" in captured.out

    def test_negative_gap_multiplier_fails(self, capsys):
        """Test that negative gap_multiplier fails validation."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {},
            "data_quality": {"gap_multiplier": -2},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "gap_multiplier (-2) must be positive" in captured.out


# ============================================================================
# Error Message Verification Tests
# ============================================================================


class TestErrorMessageQuality:
    """Test that error messages are helpful and informative."""

    def test_error_message_includes_fix_command(self, capsys):
        """Test that error messages include fix command."""
        # Arrange
        config_data = {"database": {}, "extraction": {}, "output": {}}  # Missing data_quality

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert f"{CLI_COMMAND_PYTHON} setup --force" in captured.out

    def test_error_message_includes_example(self, capsys):
        """Test that error messages include examples."""
        # Arrange
        config_data = {
            "database": {"driver": 123},  # Wrong type
            "extraction": {},
            "data_quality": {},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "Example:" in captured.out
        assert '"driver"' in captured.out

    def test_error_message_shows_found_vs_expected(self, capsys):
        """Test that error messages show what was found vs expected."""
        # Arrange
        config_data = {
            "database": {},
            "extraction": {"default_resolution": "fifty"},
            "data_quality": {},
            "output": {},
        }

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_data=config_data)

        captured = capsys.readouterr()
        assert "Found:" in captured.out
        assert "Expected:" in captured.out

    def test_error_message_includes_config_location(self, tmp_path, capsys):
        """Test that error messages include config file location."""
        # Arrange
        config_file = tmp_path / "config.json"
        config_data = {"database": {}}  # Missing sections
        config_file.write_text(json.dumps(config_data), encoding="utf-8")

        # Act & Assert
        with pytest.raises(SystemExit):
            ConfigurationManager(config_file=str(config_file))

        captured = capsys.readouterr()
        assert "Config location:" in captured.out
        assert str(config_file) in captured.out
