"""
Argument parser for PhasorPoint CLI
Handles command-line argument definition and parsing
"""

import argparse

from .banner import get_banner


class CLIArgumentParser:
    """Creates and configures the CLI argument parser."""

    def __init__(self):
        """Initialize argument parser builder."""

    def build(self) -> argparse.ArgumentParser:
        """
        Build and return configured ArgumentParser.

        Returns:
            argparse.ArgumentParser: Configured argument parser with all commands
        """
        # Build description with banner
        description = f"""{get_banner()}
PMU Data Extraction & Analysis Tool

COMMAND GROUPS:
  Configuration:    setup, config-path, config-clean, about
  Data Extraction:  extract, batch-extract
  Database Ops:     list-tables, table-info, query
"""

        parser = argparse.ArgumentParser(
            prog="phasor-cli",
            description=description,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Set up configuration files (first time setup - user-level)
  phasor-cli setup

  # Interactive setup with credential prompts
  phasor-cli setup --interactive

  # Set up project-specific configuration
  phasor-cli setup --local

  # Show configuration file locations
  phasor-cli config-path

  # Clean up configuration files
  phasor-cli config-clean

  # List all PMU tables
  phasor-cli list-tables

  # Get info about PMU 45022
  phasor-cli table-info --pmu 45022

  # Extract last 30 minutes of data
  phasor-cli extract --pmu 45022 --minutes 30

  # Extract raw data for last 24 hours
  phasor-cli extract --pmu 45022 --hours 24

  # Extract processed data with power calculations (default)
  phasor-cli extract --pmu 45022 --start "2025-08-01 00:00:00" --end "2025-08-01 01:00:00"

  # Extract raw data without processing
  phasor-cli extract --pmu 45022 --start "2025-08-01 00:00:00" --end "2025-08-01 01:00:00" --raw

  # Custom query
  phasor-cli query --sql "SELECT TOP 100 * FROM pmu_45022_1 ORDER BY ts DESC"
        """,
        )

        self._add_global_arguments(parser)
        subparsers = parser.add_subparsers(dest="command", help="Available commands")

        # Configuration commands
        self._add_setup_command(subparsers)
        self._add_config_path_command(subparsers)
        self._add_config_clean_command(subparsers)
        self._add_about_command(subparsers)

        # Data extraction commands
        self._add_extract_command(subparsers)
        self._add_batch_extract_command(subparsers)

        # Database operation commands
        self._add_list_tables_command(subparsers)
        self._add_table_info_command(subparsers)
        self._add_query_command(subparsers)

        return parser

    def _add_global_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Add global arguments (config, username, password, etc.)."""
        parser.add_argument("--config", "-c", help="Path to configuration file (config.json)")
        parser.add_argument("--username", "-u", help="Database username (or use config file)")
        parser.add_argument("--password", "-p", help="Database password (or use config file)")

    def _add_setup_command(self, subparsers) -> None:
        """Add setup command parser."""
        setup_parser = subparsers.add_parser(
            "setup",
            help="Set up configuration files (.env and config.json)",
            description="""
Set up configuration files for PhasorPoint CLI.

By default, creates configuration in the user config directory:
  - Linux/Mac: ~/.config/phasor-cli/
  - Windows: %APPDATA%/phasor-cli/

Use --local flag to create project-specific configuration in the current directory.
Use --interactive flag to be prompted for credentials (they won't be visible while typing).
            """,
        )
        setup_parser.add_argument(
            "--force", "-f", action="store_true", help="Overwrite existing files"
        )
        setup_parser.add_argument(
            "--local",
            "-l",
            action="store_true",
            help="Create config in current directory (project-specific) instead of user config directory",
        )
        setup_parser.add_argument(
            "--interactive",
            "-i",
            action="store_true",
            help="Interactively prompt for database credentials (secure password entry)",
        )
        setup_parser.add_argument(
            "--refresh-pmus",
            action="store_true",
            help="Fetch and update PMU list from database (automatically done on first setup)",
        )

    def _add_config_path_command(self, subparsers) -> None:
        """Add config-path command parser."""
        subparsers.add_parser(
            "config-path",
            help="Show configuration file locations and status",
            description="Display all configuration file locations, their priority order, and which ones are currently active.",
        )

    def _add_config_clean_command(self, subparsers) -> None:
        """Add config-clean command parser."""
        clean_parser = subparsers.add_parser(
            "config-clean",
            help="Remove configuration files",
            description="""
Remove configuration files created by the setup command.

By default, removes files from the user config directory:
  - Linux/Mac: ~/.config/phasor-cli/
  - Windows: %APPDATA%/phasor-cli/

Use --local to remove files from the current directory.
Use --all to remove files from both locations.
            """,
        )
        clean_parser.add_argument(
            "--local",
            "-l",
            action="store_true",
            help="Remove config from current directory instead of user config directory",
        )
        clean_parser.add_argument(
            "--all",
            "-a",
            action="store_true",
            help="Remove config files from both user and local directories",
        )

    def _add_about_command(self, subparsers) -> None:
        """Add about command parser."""
        subparsers.add_parser(
            "about",
            help="Show version and about information",
            description="Display version, author, repository, and feature information for PhasorPoint CLI.",
        )

    def _add_list_tables_command(self, subparsers) -> None:
        """Add list-tables command parser."""
        list_parser = subparsers.add_parser("list-tables", help="List all PMU tables")
        list_parser.add_argument(
            "--pmu",
            type=int,
            nargs="+",
            help="Specific PMU IDs to check (e.g., --pmu 45020 45019)",
        )
        list_parser.add_argument(
            "--max-pmus",
            type=int,
            default=10,
            help="Maximum PMUs to scan from config (default: 10)",
        )
        list_parser.add_argument(
            "--all", action="store_true", help="Scan all PMUs from config (may be slow)"
        )

    def _add_table_info_command(self, subparsers) -> None:
        """Add table-info command parser."""
        info_parser = subparsers.add_parser("table-info", help="Get detailed table information")
        info_parser.add_argument("--pmu", type=int, required=True, help="PMU ID")
        info_parser.add_argument(
            "--resolution", type=int, default=1, help="Data resolution (default: 1)"
        )

    def _add_extract_command(self, subparsers) -> None:
        """Add extract command parser."""
        extract_parser = subparsers.add_parser("extract", help="Extract data to Parquet file")
        extract_parser.add_argument("--pmu", type=int, required=True, help="PMU ID")
        extract_parser.add_argument(
            "--resolution", type=int, default=1, help="Data resolution (default: 1)"
        )
        extract_parser.add_argument("--start", help="Start date (YYYY-MM-DD HH:MM:SS)")
        extract_parser.add_argument("--end", help="End date (YYYY-MM-DD HH:MM:SS)")
        extract_parser.add_argument("--minutes", type=int, help="Extract last N minutes of data")
        extract_parser.add_argument("--hours", type=int, help="Extract last N hours of data")
        extract_parser.add_argument("--days", type=int, help="Extract last N days of data")
        extract_parser.add_argument("--output", "-o", help="Output file path")
        extract_parser.add_argument(
            "--format",
            choices=["parquet", "csv"],
            default="parquet",
            help="Output format (default: parquet)",
        )
        extract_parser.add_argument(
            "--processed",
            action="store_true",
            default=True,
            help="Apply data processing and power calculations (default: True)",
        )
        extract_parser.add_argument(
            "--raw",
            action="store_true",
            help="Export raw data without processing or power calculations (overrides --processed)",
        )
        extract_parser.add_argument(
            "--no-clean",
            action="store_true",
            help="Disable automatic data cleaning (NaN removal, type conversion)",
        )
        extract_parser.add_argument(
            "--chunk-size",
            type=int,
            default=15,
            help="Chunk size in minutes for large extractions (default: 15 minutes, optimized for I/O bound endpoints)",
        )
        extract_parser.add_argument(
            "--parallel",
            type=int,
            default=2,
            help="Number of parallel workers for chunked extraction (default: 2, optimal for I/O bound endpoints)",
        )
        extract_parser.add_argument(
            "--diagnostics",
            action="store_true",
            help="Enable detailed performance diagnostics and timing analysis",
        )
        extract_parser.add_argument(
            "--connection-pool",
            type=int,
            default=3,
            help="Connection pool size (default: 3, optimized for I/O bound endpoints)",
        )
        extract_parser.add_argument(
            "--verbose", "-v", action="store_true", help="Enable verbose logging output"
        )
        extract_parser.add_argument(
            "--replace",
            action="store_true",
            help="Replace existing output file if it already exists (default: skip existing files)",
        )
        extract_parser.add_argument(
            "--skip-existing",
            action="store_true",
            default=True,
            help="Skip extraction if output file already exists with matching parameters (default: True)",
        )

    def _add_batch_extract_command(self, subparsers) -> None:
        """Add batch-extract command parser."""
        batch_parser = subparsers.add_parser(
            "batch-extract", help="Extract data from multiple PMUs"
        )
        batch_parser.add_argument(
            "--pmus",
            type=str,
            required=True,
            help='Comma-separated list of PMU IDs (e.g., "45022,45028,45052")',
        )
        batch_parser.add_argument(
            "--resolution", type=int, default=1, help="Data resolution (default: 1)"
        )
        batch_parser.add_argument("--start", help="Start date (YYYY-MM-DD HH:MM:SS)")
        batch_parser.add_argument("--end", help="End date (YYYY-MM-DD HH:MM:SS)")
        batch_parser.add_argument("--minutes", type=int, help="Extract last N minutes of data")
        batch_parser.add_argument("--hours", type=int, help="Extract last N hours of data")
        batch_parser.add_argument("--days", type=int, help="Extract last N days of data")
        batch_parser.add_argument("--output-dir", "-o", help="Output directory for files")
        batch_parser.add_argument(
            "--format",
            choices=["parquet", "csv"],
            default="parquet",
            help="Output format (default: parquet)",
        )
        batch_parser.add_argument(
            "--processed",
            action="store_true",
            default=True,
            help="Apply data processing and power calculations (default)",
        )
        batch_parser.add_argument(
            "--raw",
            action="store_true",
            help="Export raw data without processing or power calculations",
        )
        batch_parser.add_argument(
            "--no-clean",
            action="store_true",
            help="Disable automatic data cleaning (NaN removal, type conversion)",
        )
        batch_parser.add_argument(
            "--chunk-size",
            type=int,
            default=15,
            help="Chunk size in minutes for large extractions (default: 15 minutes, optimized for I/O bound endpoints)",
        )
        batch_parser.add_argument(
            "--parallel",
            type=int,
            default=2,
            help="Number of parallel workers for chunked extraction (default: 2, optimal for I/O bound endpoints)",
        )
        batch_parser.add_argument(
            "--connection-pool",
            type=int,
            default=3,
            help="Connection pool size (default: 3, optimized for I/O bound endpoints)",
        )
        batch_parser.add_argument(
            "--verbose", "-v", action="store_true", help="Enable verbose logging output"
        )
        batch_parser.add_argument(
            "--replace",
            action="store_true",
            help="Replace existing output files if they already exist (default: skip existing files)",
        )
        batch_parser.add_argument(
            "--skip-existing",
            action="store_true",
            default=True,
            help="Skip extraction if output files already exist with matching parameters (default: True)",
        )

    def _add_query_command(self, subparsers) -> None:
        """Add query command parser."""
        query_parser = subparsers.add_parser("query", help="Execute custom SQL query")
        query_parser.add_argument("--sql", required=True, help="SQL query to execute")
        query_parser.add_argument("--output", "-o", help="Output file path")
        query_parser.add_argument(
            "--format",
            choices=["parquet", "csv"],
            default="parquet",
            help="Output format (default: parquet)",
        )
