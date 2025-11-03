"""
Configuration management for PhasorPoint CLI.

The ``ConfigurationManager`` class centralises loading, validation, and retrieval
of configuration data for the PhasorPoint CLI application.
"""

from __future__ import annotations

import json
import logging
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

from .config_paths import ConfigPathManager
from .constants import CLI_COMMAND_PYTHON, CONFIG_DIR_NAME
from .models import DataQualityThresholds, PMUInfo

__all__ = [
    "ConfigurationManager",
]


_EMBEDDED_DEFAULT_CONFIG: dict[str, Any] = {
    "database": {"driver": "Psymetrix PhasorPoint"},
    "extraction": {
        "default_resolution": 50,
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
    "available_pmus": [],
    "notes": {
        "discovery": f"PMU list is dynamically populated from database during setup. Use '{CLI_COMMAND_PYTHON} setup --refresh-pmus' to update.",
        "list_tables": "Use 'list-tables' command to see which PMUs are currently accessible",
    },
}


def _get_embedded_default_config() -> dict[str, Any]:
    """Return a deep copy of the embedded configuration defaults."""
    return deepcopy(_EMBEDDED_DEFAULT_CONFIG)


class ConfigurationManager:
    """High level interface for loading and querying configuration data."""

    def __init__(
        self,
        config_file: str | None = None,
        logger: logging.Logger | None = None,
        config_data: dict[str, Any] | None = None,
    ) -> None:
        self.logger = logger or logging.getLogger("phasor_cli")
        self.config_path = Path(config_file) if config_file else None
        self._provided_config = deepcopy(config_data) if config_data is not None else None
        self._config: dict[str, Any] = {}
        self._pmu_lookup: dict[int, PMUInfo] = {}
        self._load()

    # ------------------------------------------------------------------ Loading
    def _load(self) -> None:
        """Load configuration from provided dict, file or defaults."""
        config_data: dict[str, Any] | None = None

        if self._provided_config is not None:
            config_data = deepcopy(self._provided_config)
            self.logger.debug("Loaded configuration from provided dictionary")
        elif self.config_path and self.config_path.exists():
            try:
                with self.config_path.open("r", encoding="utf-8") as fh:
                    config_data = json.load(fh)
                self.logger.info(f"Loaded configuration from {self.config_path}")
            except json.JSONDecodeError as exc:
                self.logger.error(f"Invalid JSON in config file: {self.config_path}")
                self.logger.error(f"Error at line {exc.lineno}, column {exc.colno}: {exc.msg}")
                print(f"\n[ERROR] Invalid JSON format in config file: {self.config_path}")
                print(f"Error at line {exc.lineno}, column {exc.colno}: {exc.msg}\n")
                print("[FIX] Please check your config file for:")
                print("   • Missing commas between items")
                print("   • Unclosed brackets or braces")
                print("   • Invalid quotes or escape characters")
                print(f"\nOr regenerate with: {CLI_COMMAND_PYTHON} setup --force\n")
                sys.exit(1)
            except Exception as exc:  # pragma: no cover - defensive logging
                self.logger.error(f"Error loading config file: {exc}")
                print(f"\n[ERROR] Failed to load config file: {self.config_path}")
                print(f"Reason: {exc}\n")
                print("[FIX] You can:")
                print("   1. Check file permissions")
                print(f"   2. Regenerate config: {CLI_COMMAND_PYTHON} setup --force")
                print("   3. Use embedded defaults by removing the config file\n")
                sys.exit(1)
        elif self.config_path:
            self.logger.info(f"Config file not found: {self.config_path}, using embedded defaults")

        if config_data is None:
            config_data = _get_embedded_default_config()

        self._config = config_data

        # Validate configuration before building PMU lookup
        self._validate_config()

        self._build_pmu_lookup()

    def _build_pmu_lookup(self) -> None:
        """
        Create a dictionary indexed by PMU ID for quick lookups.

        Collects and reports malformed PMU entries with helpful messages.
        """
        lookup: dict[int, PMUInfo] = {}
        malformed_entries: list[tuple[Any, str]] = []  # (entry, error_type)
        duplicate_ids: dict[int, int] = {}  # pmu_id -> count

        available = self._config.get("available_pmus", [])

        if not isinstance(available, list):
            self.logger.warning(
                f"available_pmus must be a list, got {type(available).__name__}. "
                "PMU list will be empty."
            )
            self._pmu_lookup = lookup
            return

        for entry in available:
            self._process_pmu_entry(entry, lookup, malformed_entries, duplicate_ids)

        self._pmu_lookup = lookup

        # Report issues to user if any malformed entries or duplicates found
        if malformed_entries or duplicate_ids:
            self._report_pmu_validation_issues(malformed_entries, duplicate_ids, len(lookup))

    def _process_pmu_entry(
        self,
        entry: Any,
        lookup: dict[int, PMUInfo],
        malformed_entries: list[tuple[Any, str]],
        duplicate_ids: dict[int, int],
    ) -> None:
        """Process a single PMU entry."""
        try:
            info = PMUInfo.from_dict(entry)

            # Check for duplicate PMU IDs
            if info.id in lookup:
                duplicate_ids[info.id] = duplicate_ids.get(info.id, 1) + 1
                self.logger.debug(
                    f"Duplicate PMU ID {info.id}. Later entry will override earlier one."
                )

            lookup[info.id] = info

        except KeyError as e:
            field_name = e.args[0] if e.args else str(e)
            malformed_entries.append((entry, f"missing required field '{field_name}'"))
            self.logger.debug(f"PMU entry missing required field '{field_name}'")
        except TypeError as e:
            malformed_entries.append((entry, f"invalid type: {e}"))
            self.logger.debug(f"PMU entry has type error: {e}")
        except ValueError as e:
            malformed_entries.append((entry, f"invalid value: {e}"))
            self.logger.debug(f"PMU entry has invalid value: {e}")

    def _report_pmu_validation_issues(
        self,
        malformed_entries: list[tuple[Any, str]],
        duplicate_ids: dict[int, int],
        valid_count: int,
    ) -> None:
        """Report PMU validation issues to the user."""
        print("\n" + "=" * 70, file=sys.stderr)
        print("[WARNING] Issues found in PMU configuration", file=sys.stderr)
        print("=" * 70, file=sys.stderr)

        if malformed_entries:
            self._report_malformed_entries(malformed_entries)

        if duplicate_ids:
            self._report_duplicate_ids(duplicate_ids)

        print(f"\n  Successfully loaded {valid_count} valid PMU(s)", file=sys.stderr)
        print("\n  To refresh PMU list from database:", file=sys.stderr)
        print(f"    {CLI_COMMAND_PYTHON} setup --refresh-pmus", file=sys.stderr)
        print("=" * 70 + "\n", file=sys.stderr)

    def _report_malformed_entries(self, malformed_entries: list[tuple[Any, str]]) -> None:
        """Report malformed PMU entries."""
        print(f"\n  Skipped {len(malformed_entries)} malformed PMU entries:", file=sys.stderr)
        for entry, error in malformed_entries[:5]:  # Show first 5
            entry_str = str(entry)[:50] + "..." if len(str(entry)) > 50 else str(entry)
            print(f"    • {error}", file=sys.stderr)
            print(f"      Entry: {entry_str}", file=sys.stderr)

        if len(malformed_entries) > 5:
            print(f"    ... and {len(malformed_entries) - 5} more", file=sys.stderr)

        print("\n  Correct PMU format:", file=sys.stderr)
        print('    {"id": 45012, "station_name": "Station Name", "country": "US"}', file=sys.stderr)
        print("    Required fields: id, station_name", file=sys.stderr)

    def _report_duplicate_ids(self, duplicate_ids: dict[int, int]) -> None:
        """Report duplicate PMU IDs."""
        print(f"\n  Found {len(duplicate_ids)} duplicate PMU IDs:", file=sys.stderr)
        for pmu_id, count in list(duplicate_ids.items())[:5]:
            print(f"    • PMU ID {pmu_id}: appears {count} times", file=sys.stderr)
        if len(duplicate_ids) > 5:
            print(f"    ... and {len(duplicate_ids) - 5} more", file=sys.stderr)

    # --------------------------------------------------------------- Validation
    def _validate_config(self) -> None:
        """
        Validate configuration structure, types, and constraints.

        Performs 3-tier validation:
        - Tier 1: Structure and types (fails fast)
        - Tier 2: Logical constraints (fails fast)
        - Tier 3: PMU data validation (warns, continues)
        """
        self._validate_structure()
        self._validate_types()
        self._validate_constraints()
        # PMU validation happens in _build_pmu_lookup with warnings

    def _validate_structure(self) -> None:
        """Validate that required configuration sections exist and are dictionaries."""
        required_sections = ("database", "extraction", "data_quality", "output")

        for section in required_sections:
            if section not in self._config:
                self._validation_error(
                    f"Missing required configuration section: '{section}'",
                    expected="All configuration files must include: database, extraction, data_quality, output",
                    example=f'"{section}": {{\n      "field": "value"\n    }}',
                )

            if not isinstance(self._config[section], dict):
                self._validation_error(
                    f"Configuration section '{section}' must be a dictionary",
                    found=f"{type(self._config[section]).__name__}",
                    expected="dictionary/object",
                    example=f'"{section}": {{\n      "field": "value"\n    }}',
                )

    def _validate_types(self) -> None:
        """Validate data types for all configuration fields."""
        self._validate_database_types()
        self._validate_extraction_types()
        self._validate_data_quality_types()
        self._validate_output_types()

    def _validate_database_types(self) -> None:
        """Validate database section types."""
        db = self._config.get("database", {})
        if "driver" in db and not isinstance(db["driver"], str):
            self._validation_error(
                "database.driver must be a string",
                found=type(db["driver"]).__name__,
                expected="string",
                example='"driver": "Psymetrix PhasorPoint"',
            )

    def _validate_extraction_types(self) -> None:
        """Validate extraction section types."""
        extraction = self._config.get("extraction", {})

        if "default_resolution" in extraction and not isinstance(
            extraction["default_resolution"], int
        ):
            self._validation_error(
                "extraction.default_resolution must be an integer",
                found=type(extraction["default_resolution"]).__name__,
                expected="integer",
                example='"default_resolution": 50',
            )

        if "default_clean" in extraction and not isinstance(extraction["default_clean"], bool):
            self._validation_error(
                "extraction.default_clean must be a boolean",
                found=type(extraction["default_clean"]).__name__,
                expected="boolean (true/false)",
                example='"default_clean": true',
            )

        if "timezone_handling" in extraction:
            if not isinstance(extraction["timezone_handling"], str):
                self._validation_error(
                    "extraction.timezone_handling must be a string",
                    found=type(extraction["timezone_handling"]).__name__,
                    expected="string",
                    example='"timezone_handling": "machine_timezone"',
                )
            elif extraction["timezone_handling"] not in ("machine_timezone", "utc", "local"):
                self._validation_error(
                    f"extraction.timezone_handling has invalid value: '{extraction['timezone_handling']}'",
                    expected="one of: machine_timezone, utc, local",
                    example='"timezone_handling": "machine_timezone"',
                )

    def _validate_data_quality_types(self) -> None:
        """Validate data_quality section types."""
        dq = self._config.get("data_quality", {})

        for field in ("frequency_min", "frequency_max", "null_threshold_percent", "gap_multiplier"):
            if field in dq and not isinstance(dq[field], (int, float)):
                self._validation_error(
                    f"data_quality.{field} must be a number",
                    found=type(dq[field]).__name__,
                    expected="number (int or float)",
                    example=f'"{field}": 50',
                )

    def _validate_output_types(self) -> None:
        """Validate output section types."""
        output = self._config.get("output", {})

        if "default_output_dir" in output:
            if not isinstance(output["default_output_dir"], str):
                self._validation_error(
                    "output.default_output_dir must be a string",
                    found=type(output["default_output_dir"]).__name__,
                    expected="string",
                    example='"default_output_dir": "data_exports"',
                )
            elif not output["default_output_dir"].strip():
                self._validation_error(
                    "output.default_output_dir cannot be empty",
                    expected="non-empty string",
                    example='"default_output_dir": "data_exports"',
                )

        if "compression" in output:
            if not isinstance(output["compression"], str):
                self._validation_error(
                    "output.compression must be a string",
                    found=type(output["compression"]).__name__,
                    expected="string",
                    example='"compression": "snappy"',
                )
            elif output["compression"] not in ("snappy", "gzip", "none"):
                self._validation_error(
                    f"output.compression has invalid value: '{output['compression']}'",
                    expected="one of: snappy, gzip, none",
                    example='"compression": "snappy"',
                )

    def _validate_constraints(self) -> None:
        """Validate logical constraints between configuration values."""
        # Extraction constraints
        extraction = self._config.get("extraction", {})

        if "default_resolution" in extraction:
            resolution = extraction["default_resolution"]
            if resolution <= 0:
                self._validation_error(
                    f"extraction.default_resolution must be positive (got {resolution})",
                    expected="positive integer (e.g., 50, 100)",
                    example='"default_resolution": 50',
                )
            elif resolution > 1000:
                self.logger.warning(
                    f"extraction.default_resolution is unusually high ({resolution}). "
                    "Typical values are 1-1000."
                )

        # Data quality constraints
        dq = self._config.get("data_quality", {})

        freq_min = dq.get("frequency_min")
        freq_max = dq.get("frequency_max")

        if freq_min is not None and freq_max is not None and freq_max <= freq_min:
            self._validation_error(
                f"data_quality.frequency_max ({freq_max}) must be greater than frequency_min ({freq_min})",
                expected="frequency_max > frequency_min",
                example='"frequency_min": 45, "frequency_max": 65',
            )

        if freq_min is not None and (freq_min < 0 or freq_min > 100):
            self._validation_error(
                f"data_quality.frequency_min ({freq_min}) must be between 0 and 100",
                expected="value between 0-100 Hz",
                example='"frequency_min": 45',
            )

        if freq_max is not None and (freq_max < 0 or freq_max > 100):
            self._validation_error(
                f"data_quality.frequency_max ({freq_max}) must be between 0 and 100",
                expected="value between 0-100 Hz",
                example='"frequency_max": 65',
            )

        null_threshold = dq.get("null_threshold_percent")
        if null_threshold is not None and (null_threshold < 0 or null_threshold > 100):
            self._validation_error(
                f"data_quality.null_threshold_percent ({null_threshold}) must be between 0 and 100",
                expected="percentage value 0-100",
                example='"null_threshold_percent": 50',
            )

        gap_multiplier = dq.get("gap_multiplier")
        if gap_multiplier is not None and gap_multiplier <= 0:
            self._validation_error(
                f"data_quality.gap_multiplier ({gap_multiplier}) must be positive",
                expected="positive number",
                example='"gap_multiplier": 5',
            )

    def _validation_error(
        self, message: str, found: str = "", expected: str = "", example: str = ""
    ) -> None:
        """
        Print a formatted validation error message and exit.

        Args:
            message: The error message
            found: What was found (optional)
            expected: What was expected (optional)
            example: Example of correct format (optional)
        """
        self.logger.error(f"Configuration validation failed: {message}")

        print(f"\n[ERROR] Invalid configuration: {message}")

        if found:
            print(f"  Found: {found}")
        if expected:
            print(f"  Expected: {expected}")

        if example:
            print("\n  Example:")
            for line in example.split("\n"):
                print(f"    {line}")

        config_location = self.config_path or "provided configuration"
        print(f"\n  Config location: {config_location}")
        print("\n[FIX] To regenerate a valid configuration:")
        print(f"   {CLI_COMMAND_PYTHON} setup --force")
        print()

        sys.exit(1)

    # ------------------------------------------------------------------ Helpers
    @property
    def config(self) -> dict[str, Any]:
        """Return a deep copy of the loaded configuration."""
        return deepcopy(self._config)

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve configuration values with optional dotted path access."""
        if "." not in key:
            return deepcopy(self._config.get(key, default))

        current: Any = self._config
        for part in key.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return default
        return deepcopy(current)

    def get_database_config(self) -> dict[str, Any]:
        return deepcopy(self._config.get("database", {}))

    def get_extraction_config(self) -> dict[str, Any]:
        return deepcopy(self._config.get("extraction", {}))

    def get_data_quality_thresholds(self) -> DataQualityThresholds:
        data = self._config.get("data_quality", {}) or {}
        thresholds = DataQualityThresholds(
            frequency_min=data.get("frequency_min", 45),
            frequency_max=data.get("frequency_max", 65),
            null_threshold_percent=data.get("null_threshold_percent", 50),
            gap_multiplier=data.get("gap_multiplier", 5),
        )
        thresholds.validate()
        return thresholds

    def get_pmu_info(self, pmu_id: int) -> PMUInfo | None:
        return self._pmu_lookup.get(int(pmu_id))

    def get_all_pmu_ids(self) -> list[int]:
        return sorted(self._pmu_lookup.keys())

    def validate(self) -> None:
        """Perform structural validation of the configuration."""
        required_sections = ("database", "extraction", "data_quality", "output")
        missing = [section for section in required_sections if section not in self._config]
        if missing:
            raise ValueError(f"Missing required configuration sections: {', '.join(missing)}")

        # Validate thresholds to ensure numeric values are sane.
        self.get_data_quality_thresholds()

        if not self._pmu_lookup:
            self.logger.warning(
                "Configuration does not define any available PMUs. "
                f"Run '{CLI_COMMAND_PYTHON} setup --refresh-pmus' to populate PMU list from database."
            )

    # -------------------------------------------------------------- Setup files
    @staticmethod
    def _fetch_and_populate_pmus(
        config_file: Path,
        env_file: Path,
        is_new_config: bool,
        logger: logging.Logger,
    ) -> None:
        """
        Fetch PMU list from database and populate config file.

        Args:
            config_file: Path to config.json file
            env_file: Path to .env file with credentials
            is_new_config: Whether this is a new config (vs. refreshing existing)
            logger: Logger instance
        """
        from dotenv import load_dotenv  # noqa: PLC0415 - late import

        from .connection_manager import ConnectionManager  # noqa: PLC0415 - late import
        from .pmu_metadata import (  # noqa: PLC0415 - late import
            fetch_pmu_metadata_from_database,
            merge_pmu_metadata,
        )

        # Load credentials from .env file
        load_dotenv(dotenv_path=env_file)

        # Use existing ConnectionManager to handle credentials and connection string
        # Create a minimal config manager for ConnectionManager
        temp_config_manager = ConfigurationManager(config_file=str(config_file), logger=logger)
        conn_manager = ConnectionManager(temp_config_manager, logger)
        conn_manager.setup_credentials()

        # Check if credentials are available
        if not conn_manager.is_configured:
            logger.warning("Database credentials not fully configured in .env file")
            logger.info(
                f"PMU list not populated. Run '{CLI_COMMAND_PYTHON} setup --refresh-pmus' after configuring credentials."
            )
            return

        # Try to fetch PMU metadata from database
        try:
            logger.info("Fetching PMU metadata from database...")
            # Create a connection pool with single connection for metadata fetch
            connection_pool = conn_manager.create_connection_pool(pool_size=1)
            fetched_pmus = fetch_pmu_metadata_from_database(connection_pool, logger)

            # Load existing config
            with config_file.open("r", encoding="utf-8") as fh:
                config_data = json.load(fh)

            # Merge or replace PMU data
            if is_new_config:
                # New config: replace empty list with fetched PMUs
                config_data["available_pmus"] = fetched_pmus
                logger.info(f"Populated config with {len(fetched_pmus)} PMUs from database")
            else:
                # Existing config: merge with existing PMUs
                existing_pmus = config_data.get("available_pmus", [])
                merged_pmus = merge_pmu_metadata(existing_pmus, fetched_pmus)
                config_data["available_pmus"] = merged_pmus
                logger.info(
                    f"Merged PMU metadata: {len(merged_pmus)} total PMUs ({len(fetched_pmus)} fetched)"
                )

            # Write updated config back to file
            with config_file.open("w", encoding="utf-8") as fh:
                json.dump(config_data, fh, indent=2)

        except Exception as exc:
            logger.warning(f"Could not fetch PMU list from database: {exc}")
            logger.info(
                f"Created config with empty PMU list. Run '{CLI_COMMAND_PYTHON} setup --refresh-pmus' to populate PMUs."
            )

    @staticmethod
    def setup_configuration_files(  # noqa: PLR0912, PLR0915
        local: bool = False,
        *,
        force: bool = False,
        interactive: bool = False,
        refresh_pmus: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Create or refresh configuration files.

        Args:
            local: If True, create files in current directory. If False, create in user config directory.
            force: If True, overwrite existing files.
            interactive: If True, prompt user for credentials interactively.
            refresh_pmus: If True, fetch and update PMU list from database. Automatically True for new configs.
            logger: Optional logger instance.
        """
        log = logger or logging.getLogger("setup")
        log.info("Setting up configuration files...")

        # Prepare .env content
        if interactive:
            env_template = ConfigurationManager._create_interactive_env_content(log)
        else:
            env_template = """# PhasorPoint Database Configuration
# REQUIRED: Fill in your actual database credentials

DB_HOST=your_database_host_here
DB_PORT=your_database_port_here
DB_NAME=your_database_name_here
DB_USERNAME=your_username_here
DB_PASSWORD=your_password_here

# Optional: Application settings
LOG_LEVEL=INFO
DEFAULT_OUTPUT_DIR=data_exports
"""

        # Determine target directory
        path_manager = ConfigPathManager()
        if local:
            config_dir = Path.cwd()
            location_desc = "current directory (project-specific)"
        else:
            config_dir = path_manager.get_user_config_dir()
            location_desc = f"user config directory ({config_dir})"

        env_file = config_dir / ".env"
        config_file = config_dir / "config.json"

        log.info(f"Target location: {location_desc}")

        # Create .env file
        if env_file.exists() and not force:
            log.warning(f".env file already exists at {env_file}. Use --force to overwrite.")
        else:
            try:
                env_file.write_text(env_template, encoding="utf-8")
                log.info(f"Created .env file: {env_file}")
            except Exception as exc:  # pragma: no cover - defensive logging
                log.error(f"Error creating .env file: {exc}")
                return

        # Create config.json file
        config_is_new = not config_file.exists() or force
        if config_file.exists() and not force:
            log.info(f"config.json already exists at {config_file}, using existing file")
        else:
            default_config = _get_embedded_default_config()
            try:
                config_file.write_text(json.dumps(default_config, indent=2), encoding="utf-8")
                log.info(f"Created config.json file: {config_file}")
            except Exception as exc:  # pragma: no cover - defensive logging
                log.error(f"Error creating config.json file: {exc}")
                return

        # Fetch PMUs from database if this is a new config or refresh is requested
        should_refresh_pmus = config_is_new or refresh_pmus
        if should_refresh_pmus:
            ConfigurationManager._fetch_and_populate_pmus(config_file, env_file, config_is_new, log)

        print("\n" + "=" * 70)
        print("Setup Complete!")
        print("=" * 70)

        if local:
            print("\nConfiguration Type: PROJECT-SPECIFIC (Local)")
            print(f"Location: {config_dir}")
            print("\nThese files will only be used when running commands from this directory.")
        else:
            print("\nConfiguration Type: USER-LEVEL (Global)")
            print(f"Location: {config_dir}")
            print(
                "\nThese files will be used from any directory unless overridden by local configs."
            )

        print("\nFiles created/updated:")
        print(f"  {env_file}")
        print(f"  {config_file}")

        print("\n" + "-" * 70)
        print("Next Steps:")
        print("-" * 70)
        print("\n1. Edit your .env file with actual credentials:")
        print(f"   {env_file}")
        print("\n   Replace placeholder values:")
        print("   DB_USERNAME=your_actual_username")
        print("   DB_PASSWORD=your_actual_password")
        print("   DB_HOST=your_database_host")
        print("   DB_PORT=your_database_port")
        print("   DB_NAME=your_database_name")

        print("\n2. Test your database connection:")
        print(f"   {CLI_COMMAND_PYTHON} list-tables")

        print("\n3. Extract some data:")
        print(f"   {CLI_COMMAND_PYTHON} extract --pmu 45022 --hours 1")

        print("\n" + "-" * 70)
        print("Configuration Priority:")
        print("-" * 70)
        print("1. Environment variables (highest priority)")
        print("2. Local project config (./config.json, ./.env)")
        print(f"3. User config (~/.config/{CONFIG_DIR_NAME}/ or %APPDATA%/{CONFIG_DIR_NAME}/)")
        print("4. Embedded defaults (lowest priority)")

        print("\n" + "-" * 70)
        print("Security Reminder:")
        print("-" * 70)
        print("- Never commit .env files with real credentials to version control")
        print("- Add .env to your .gitignore file")
        print("- Use environment variables in production environments")

        if not local:
            print("\n" + "-" * 70)
            print("Project-Specific Configuration:")
            print("-" * 70)
            print("To create project-specific configs that override user defaults:")
            print(f"   {CLI_COMMAND_PYTHON} setup --local")

    @staticmethod
    def _create_interactive_env_content(logger: logging.Logger | None = None) -> str:
        """
        Interactively prompt user for database credentials.

        Args:
            logger: Optional logger instance.

        Returns:
            String containing .env file content with user-provided values.
        """
        import getpass  # noqa: PLC0415 - interactive import only used during setup

        log = logger or logging.getLogger("setup")

        print("\n" + "=" * 70)
        print("Interactive Database Configuration")
        print("=" * 70)
        print("\nPlease provide your database connection details:")
        print("(Press Enter to skip optional fields)\n")

        try:
            db_host = input("Database Host (e.g., localhost, 10.0.0.5): ").strip()
            while not db_host:
                print("  Error: Database host is required")
                db_host = input("Database Host (e.g., localhost, 10.0.0.5): ").strip()

            db_port = input("Database Port (e.g., 1433): ").strip()
            while not db_port:
                print("  Error: Database port is required")
                db_port = input("Database Port (e.g., 1433): ").strip()

            db_name = input("Database Name (e.g., PhasorPoint): ").strip()
            while not db_name:
                print("  Error: Database name is required")
                db_name = input("Database Name (e.g., PhasorPoint): ").strip()

            db_username = input("Username (e.g., phasor_user): ").strip()
            while not db_username:
                print("  Error: Username is required")
                db_username = input("Username (e.g., phasor_user): ").strip()

            # Use getpass for password to hide input
            db_password = getpass.getpass("Password (hidden): ").strip()
            while not db_password:
                print("  Error: Password is required")
                db_password = getpass.getpass("Password (hidden): ").strip()

            # Optional settings
            log_level = input("Log Level [optional, default: INFO]: ").strip() or "INFO"
            output_dir = (
                input("Default Output Directory [optional, default: data_exports]: ").strip()
                or "data_exports"
            )

            print("\n✓ Configuration captured successfully!\n")

            # Build .env content
            return f"""# PhasorPoint Database Configuration
# Generated interactively on {Path.cwd()}

DB_HOST={db_host}
DB_PORT={db_port}
DB_NAME={db_name}
DB_USERNAME={db_username}
DB_PASSWORD={db_password}

# Optional: Application settings
LOG_LEVEL={log_level}
DEFAULT_OUTPUT_DIR={output_dir}
"""

        except (KeyboardInterrupt, EOFError):
            log.warning("\nInteractive setup cancelled by user")
            print("\n\nSetup cancelled. Using template instead.")
            return """# PhasorPoint Database Configuration
# REQUIRED: Fill in your actual database credentials

DB_HOST=your_database_host_here
DB_PORT=your_database_port_here
DB_NAME=your_database_name_here
DB_USERNAME=your_username_here
DB_PASSWORD=your_password_here

# Optional: Application settings
LOG_LEVEL=INFO
DEFAULT_OUTPUT_DIR=data_exports
"""

    @staticmethod
    def cleanup_configuration_files(
        local: bool = False,
        *,
        all_locations: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Remove configuration files.

        Args:
            local: If True, remove files from current directory. If False, remove from user config directory.
            all_locations: If True, remove files from both locations.
            logger: Optional logger instance.
        """
        log = logger or logging.getLogger("setup")
        log.info("Cleaning up configuration files...")

        path_manager = ConfigPathManager()
        files_removed = []
        files_not_found = []

        def remove_file(file_path: Path, location: str) -> None:
            """Helper to remove a file and track result."""
            if file_path.exists():
                try:
                    # Prefer pathlib's unlink for filesystem operations
                    file_path.unlink()
                    files_removed.append((str(file_path), location))
                    log.info(f"Removed: {file_path}")
                except Exception as e:
                    log.error(f"Failed to remove {file_path}: {e}")
            else:
                files_not_found.append((str(file_path), location))

        # Determine which locations to clean
        if all_locations:
            locations = [("local", Path.cwd()), ("user", path_manager.get_user_config_dir())]
        elif local:
            locations = [("local", Path.cwd())]
        else:
            locations = [("user", path_manager.get_user_config_dir())]

        # Remove files from selected locations
        for location_name, config_dir in locations:
            env_file = config_dir / ".env"
            config_file = config_dir / "config.json"

            remove_file(env_file, location_name)
            remove_file(config_file, location_name)

        # Display results
        print("\n" + "=" * 70)
        print("Configuration Cleanup Complete")
        print("=" * 70)

        if files_removed:
            print(f"\nRemoved {len(files_removed)} file(s):")
            for file_path, location in files_removed:
                print(f"  [{location.upper()}] {file_path}")

        if files_not_found:
            print(f"\nNot found ({len(files_not_found)} file(s)):")
            for file_path, location in files_not_found:
                print(f"  [{location.upper()}] {file_path}")

        if not files_removed and not files_not_found:
            print("\nNo configuration files to remove.")

        print("\n" + "-" * 70)
        print("Note: Embedded defaults will still be used by the application.")
        print("To create new configuration files, run:")
        print(f"   {CLI_COMMAND_PYTHON} setup")
        print("-" * 70)
