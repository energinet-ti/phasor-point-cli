"""
Cross-platform configuration path utilities for PhasorPoint CLI.

Provides standardized configuration directory locations following OS conventions:
- Linux/Mac: ~/.config/phasor-cli/
- Windows: %APPDATA%/phasor-cli/
"""

import os
import sys
from pathlib import Path
from typing import Any


class ConfigPathManager:
    """
    Manages configuration file paths and locations across different platforms.

    Provides cross-platform support for finding and managing configuration files
    with a priority-based system.
    """

    def __init__(self):
        """Initialize the configuration path manager."""
        self._user_config_dir: Path | None = None

    def get_user_config_dir(self) -> Path:
        """
        Get the platform-appropriate user configuration directory.

        Returns:
            Path to user configuration directory (creates if doesn't exist)

        Platform-specific locations:
            - Linux/Mac: ~/.config/phasor-cli/
            - Windows: %APPDATA%/phasor-cli/
        """
        if self._user_config_dir is not None:
            return self._user_config_dir

        if sys.platform == "win32":
            # Windows: Use APPDATA
            base = os.environ.get("APPDATA")
            if not base:
                # Fallback to USERPROFILE if APPDATA not set
                base = os.environ.get("USERPROFILE", str(Path.home()))
                config_dir = Path(base) / "phasor-cli"
            else:
                config_dir = Path(base) / "phasor-cli"
        else:
            # Linux/Mac: Use XDG_CONFIG_HOME or ~/.config
            xdg_config = os.environ.get("XDG_CONFIG_HOME")
            if xdg_config:
                config_dir = Path(xdg_config) / "phasor-cli"
            else:
                config_dir = Path.home() / ".config" / "phasor-cli"

        # Create directory if it doesn't exist
        config_dir.mkdir(parents=True, exist_ok=True)
        self._user_config_dir = config_dir
        return config_dir

    def get_user_config_file(self) -> Path:
        """Get the path to the user-level config.json file."""
        return self.get_user_config_dir() / "config.json"

    def get_user_env_file(self) -> Path:
        """Get the path to the user-level .env file."""
        return self.get_user_config_dir() / ".env"

    def get_local_config_file(self) -> Path:
        """Get the path to the local project config.json file."""
        return Path.cwd() / "config.json"

    def get_local_env_file(self) -> Path:
        """Get the path to the local project .env file."""
        return Path.cwd() / ".env"

    def find_config_file(self, config_arg: str | None = None) -> Path | None:
        """
        Find the configuration file using priority order.

        Priority:
            1. Explicitly provided config_arg
            2. Local project config (./config.json)
            3. User config (~/.config/phasor-cli/config.json)
            4. None (will use embedded defaults)

        Args:
            config_arg: Explicitly provided config file path

        Returns:
            Path to config file if found, None otherwise
        """
        # Priority 1: Explicitly provided config
        if config_arg:
            config_path = Path(config_arg)
            if config_path.exists():
                return config_path
            return None

        # Priority 2: Local project config
        local_config = self.get_local_config_file()
        if local_config.exists():
            return local_config

        # Priority 3: User config
        user_config = self.get_user_config_file()
        if user_config.exists():
            return user_config

        # Priority 4: None (will use embedded defaults)
        return None

    def find_env_file(self) -> Path | None:
        """
        Find the .env file using priority order.

        Priority:
            1. Local project .env (./.env)
            2. User .env (~/.config/phasor-cli/.env)
            3. None

        Returns:
            Path to .env file if found, None otherwise
        """
        # Priority 1: Local project .env
        local_env = self.get_local_env_file()
        if local_env.exists():
            return local_env

        # Priority 2: User .env
        user_env = self.get_user_env_file()
        if user_env.exists():
            return user_env

        # Priority 3: None
        return None

    def get_config_locations_info(self) -> dict[str, Any]:
        """
        Get information about all config file locations and their status.

        Returns:
            Dictionary with config location information
        """
        user_config = self.get_user_config_file()
        user_env = self.get_user_env_file()
        local_config = self.get_local_config_file()
        local_env = self.get_local_env_file()

        return {
            "user_config_dir": self.get_user_config_dir(),
            "user_config": {"path": user_config, "exists": user_config.exists(), "priority": 3},
            "user_env": {"path": user_env, "exists": user_env.exists(), "priority": 2},
            "local_config": {"path": local_config, "exists": local_config.exists(), "priority": 2},
            "local_env": {"path": local_env, "exists": local_env.exists(), "priority": 1},
            "active_config": self.find_config_file(),
            "active_env": self.find_env_file(),
        }
