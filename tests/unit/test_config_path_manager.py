"""
Unit tests for the ConfigPathManager class.
"""

import sys
from pathlib import Path

from phasor_point_cli.config_paths import ConfigPathManager
from phasor_point_cli.constants import CONFIG_DIR_NAME


def test_config_path_manager_initializes():
    # Arrange & Act
    manager = ConfigPathManager()

    # Assert
    assert manager is not None


def test_get_user_config_dir_creates_directory(tmp_path, monkeypatch):
    # Arrange
    manager = ConfigPathManager()

    # Mock the home directory to use tmp_path
    if sys.platform == "win32":
        monkeypatch.setenv("APPDATA", str(tmp_path))
        expected_dir = tmp_path / CONFIG_DIR_NAME
    else:
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("XDG_CONFIG_HOME", "")  # Clear XDG_CONFIG_HOME
        expected_dir = tmp_path / ".config" / CONFIG_DIR_NAME

    # Clear cached value
    manager._user_config_dir = None

    # Act
    config_dir = manager.get_user_config_dir()

    # Assert
    assert config_dir.exists()
    assert config_dir == expected_dir


def test_get_user_config_file_returns_path():
    # Arrange
    manager = ConfigPathManager()

    # Act
    config_file = manager.get_user_config_file()

    # Assert
    assert config_file.name == "config.json"
    assert CONFIG_DIR_NAME in str(config_file)


def test_get_user_env_file_returns_path():
    # Arrange
    manager = ConfigPathManager()

    # Act
    env_file = manager.get_user_env_file()

    # Assert
    assert env_file.name == ".env"
    assert CONFIG_DIR_NAME in str(env_file)


def test_get_local_config_file_uses_cwd():
    # Arrange
    manager = ConfigPathManager()

    # Act
    config_file = manager.get_local_config_file()

    # Assert
    assert config_file == Path.cwd() / "config.json"


def test_get_local_env_file_uses_cwd():
    # Arrange
    manager = ConfigPathManager()

    # Act
    env_file = manager.get_local_env_file()

    # Assert
    assert env_file == Path.cwd() / ".env"


def test_find_config_file_prefers_explicit_arg(tmp_path):
    # Arrange
    manager = ConfigPathManager()
    explicit_config = tmp_path / "explicit.json"
    explicit_config.write_text('{"test": true}', encoding="utf-8")

    # Act
    result = manager.find_config_file(str(explicit_config))

    # Assert
    assert result == explicit_config


def test_find_config_file_returns_none_for_nonexistent_explicit():
    # Arrange
    manager = ConfigPathManager()

    # Act
    result = manager.find_config_file("/nonexistent/config.json")

    # Assert
    assert result is None


def test_find_config_file_prefers_local_over_user(tmp_path, monkeypatch):
    # Arrange
    manager = ConfigPathManager()

    # Create local config
    monkeypatch.chdir(tmp_path)
    local_config = tmp_path / "config.json"
    local_config.write_text('{"local": true}', encoding="utf-8")

    # Create user config
    user_dir = tmp_path / "user_config"
    user_dir.mkdir(parents=True, exist_ok=True)
    user_config = user_dir / "config.json"
    user_config.write_text('{"user": true}', encoding="utf-8")
    manager._user_config_dir = user_dir

    # Act
    result = manager.find_config_file()

    # Assert - should prefer local
    assert result == local_config


def test_find_config_file_falls_back_to_user(tmp_path, monkeypatch):
    # Arrange
    manager = ConfigPathManager()

    # Change to directory with no local config
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    monkeypatch.chdir(empty_dir)

    # Create user config only
    user_dir = tmp_path / "user_config"
    user_dir.mkdir(parents=True, exist_ok=True)
    user_config = user_dir / "config.json"
    user_config.write_text('{"user": true}', encoding="utf-8")
    manager._user_config_dir = user_dir

    # Act
    result = manager.find_config_file()

    # Assert
    assert result == user_config


def test_find_config_file_returns_none_when_no_config_exists(tmp_path, monkeypatch):
    # Arrange
    manager = ConfigPathManager()

    # Change to empty directory
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    monkeypatch.chdir(empty_dir)

    # Set user dir to empty location
    user_dir = tmp_path / "user_config"
    user_dir.mkdir(parents=True, exist_ok=True)
    manager._user_config_dir = user_dir

    # Act
    result = manager.find_config_file()

    # Assert
    assert result is None


def test_find_env_file_prefers_local_over_user(tmp_path, monkeypatch):
    # Arrange
    manager = ConfigPathManager()

    # Create local .env
    monkeypatch.chdir(tmp_path)
    local_env = tmp_path / ".env"
    local_env.write_text("DB_HOST=local", encoding="utf-8")

    # Create user .env
    user_dir = tmp_path / "user_config"
    user_dir.mkdir(parents=True, exist_ok=True)
    user_env = user_dir / ".env"
    user_env.write_text("DB_HOST=user", encoding="utf-8")
    manager._user_config_dir = user_dir

    # Act
    result = manager.find_env_file()

    # Assert - should prefer local
    assert result == local_env


def test_find_env_file_returns_none_when_no_env_exists(tmp_path, monkeypatch):
    # Arrange
    manager = ConfigPathManager()

    # Change to empty directory
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    monkeypatch.chdir(empty_dir)

    # Set user dir to empty location
    user_dir = tmp_path / "user_config"
    user_dir.mkdir(parents=True, exist_ok=True)
    manager._user_config_dir = user_dir

    # Act
    result = manager.find_env_file()

    # Assert
    assert result is None


def test_get_config_locations_info_returns_complete_dict(tmp_path, monkeypatch):
    # Arrange
    manager = ConfigPathManager()

    # Create test environment
    monkeypatch.chdir(tmp_path)
    local_config = tmp_path / "config.json"
    local_config.write_text('{"test": true}', encoding="utf-8")

    user_dir = tmp_path / "user_config"
    user_dir.mkdir(parents=True, exist_ok=True)
    manager._user_config_dir = user_dir

    # Act
    info = manager.get_config_locations_info()

    # Assert
    assert "user_config_dir" in info
    assert "user_config" in info
    assert "user_env" in info
    assert "local_config" in info
    assert "local_env" in info
    assert "active_config" in info
    assert "active_env" in info

    # Verify structure of sub-dicts
    assert info["local_config"]["exists"] is True
    assert info["local_config"]["path"] == local_config
    assert info["active_config"] == local_config
