"""
Argument parser for PhasorPoint CLI
Handles command-line argument definition and parsing
"""

import argparse

from .banner import get_banner
from .constants import CLI_COMMAND_PYTHON, CONFIG_DIR_NAME


class BetterHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Custom formatter with better indentation for help text."""

    def __init__(self, prog):
        """Initialize formatter with improved max_help_position."""
        super().__init__(prog, max_help_position=35)


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
  Configuration:    setup, config, about
  Data Extraction:  extract, batch-extract
  Database Ops:     list-tables, table-info, query
"""

        parser = argparse.ArgumentParser(
            prog=CLI_COMMAND_PYTHON,
            description=description,
            formatter_class=BetterHelpFormatter,
            epilog=f"""
Quick Start:
  {CLI_COMMAND_PYTHON} setup                    # First time setup (interactive)
  {CLI_COMMAND_PYTHON} list-tables              # See available PMU tables
  {CLI_COMMAND_PYTHON} about                    # Show version and features

Common Examples:
  {CLI_COMMAND_PYTHON} extract --pmu 45022 --hours 1                    # Last hour (50Hz, CSV)
  {CLI_COMMAND_PYTHON} extract --pmu 45022 --start "2025-08-01 10:00:00" --end "2025-08-01 11:00:00"
  {CLI_COMMAND_PYTHON} extract --pmu 45022 --hours 24 --format parquet  # 24 hours as Parquet
  {CLI_COMMAND_PYTHON} batch-extract --pmus "45022,45028" --hours 24    # Multiple PMUs
  {CLI_COMMAND_PYTHON} table-info --pmu 45022                           # Table details

More help: {CLI_COMMAND_PYTHON} <command> --help
        """,
        )

        self._add_global_arguments(parser)
        subparsers = parser.add_subparsers(dest="command", help="Available commands")

        # Configuration commands
        self._add_setup_command(subparsers)
        self._add_config_command(subparsers)
        self._add_about_command(subparsers)
        self._add_aboot_command(subparsers)  # Hidden easter egg

        # Data extraction commands (moved up for better discoverability)
        self._add_extract_command(subparsers)
        self._add_batch_extract_command(subparsers)

        # Database operation commands
        self._add_list_tables_command(subparsers)
        self._add_table_info_command(subparsers)
        self._add_query_command(subparsers)

        # Hide aboot from help display while keeping it functional
        self._hide_easter_egg_from_help(subparsers)

        return parser

    def _hide_easter_egg_from_help(self, subparsers) -> None:
        """Hide the aboot command from help display while keeping it functional."""
        # Store the original choices dict but create a filtered view for display
        if hasattr(subparsers, "choices") and "aboot" in subparsers.choices:
            original_choices = subparsers.choices

            # Create a wrapper class that hides 'aboot' during iteration
            class FilteredChoicesView(dict):
                """A dict wrapper that hides specific keys from iteration."""

                def __init__(self, wrapped_dict, hidden_keys):
                    super().__init__(wrapped_dict)
                    self._hidden = set(hidden_keys)

                def __iter__(self):
                    return (k for k in super().__iter__() if k not in self._hidden)

                def keys(self):
                    return [k for k in super() if k not in self._hidden]

                def __str__(self):
                    return "{" + ",".join(self.keys()) + "}"

            # Replace choices with filtered view
            subparsers.choices = FilteredChoicesView(original_choices, ["aboot"])

            # Also hide from _get_subactions for help formatting
            if hasattr(subparsers, "_get_subactions"):
                original_get_subactions = subparsers._get_subactions

                def filtered_get_subactions():
                    subactions = original_get_subactions()
                    return [a for a in subactions if getattr(a, "dest", None) != "aboot"]

                subparsers._get_subactions = filtered_get_subactions

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
            description=f"""
Set up configuration files for PhasorPoint CLI.

By default, creates configuration in the user config directory:
  - Linux/Mac: ~/.config/{CONFIG_DIR_NAME}/
  - Windows: %APPDATA%/{CONFIG_DIR_NAME}/

Interactive mode is enabled by default - you'll be prompted for credentials securely.
Use --no-interactive to skip prompts and create template files instead.
Use --local flag to create project-specific configuration in the current directory.
            """,
        )
        setup_parser.add_argument(
            "--force", "-f", action="store_true", help="Overwrite existing files"
        )
        setup_parser.add_argument(
            "--local",
            "-l",
            action="store_true",
            help="Create project-specific config in current directory",
        )

        setup_parser.add_argument(
            "--no-interactive",
            action="store_false",
            dest="interactive",
            help="Skip interactive prompts, use template files instead",
        )
        setup_parser.set_defaults(interactive=True)

        setup_parser.add_argument(
            "--refresh-pmus",
            action="store_true",
            help="Fetch and update PMU list from database",
        )

    def _add_config_command(self, subparsers) -> None:
        """Add config command parser (combines config-path and config-clean functionality)."""
        config_parser = subparsers.add_parser(
            "config",
            help="Show or manage configuration files",
            description="""
Manage and inspect configuration files.

By default, displays all configuration file locations, their priority order,
and which ones are currently active.

Use --clean flag to remove configuration files instead.
            """,
        )
        config_parser.add_argument(
            "--clean",
            action="store_true",
            help="Remove configuration files",
        )
        config_parser.add_argument(
            "--local",
            "-l",
            action="store_true",
            help="Target local config in current directory",
        )
        config_parser.add_argument(
            "--all",
            "-a",
            action="store_true",
            help="Target all config locations (both user and local)",
        )

    def _add_about_command(self, subparsers) -> None:
        """Add about command parser."""
        subparsers.add_parser(
            "about",
            help="Show version and about information",
            description="Display version, author, repository, and feature information for PhasorPoint CLI.",
        )

    def _add_aboot_command(self, subparsers) -> None:
        """Add hidden aboot command parser (easter egg)."""
        subparsers.add_parser(
            "aboot",
            help=argparse.SUPPRESS,  # Hide from help text
            description="Hidden easter egg command.",
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
            "--resolution", type=int, default=50, help="Data resolution (default: 50)"
        )

    def _add_extract_command(self, subparsers) -> None:
        """Add extract command parser."""
        extract_parser = subparsers.add_parser(
            "extract", help="Extract data to CSV or Parquet file"
        )
        extract_parser.add_argument("--pmu", type=int, required=True, help="PMU ID")
        extract_parser.add_argument(
            "--resolution", type=int, default=50, help="Data resolution (default: 50)"
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
            default="csv",
            help="Output format (default: csv)",
        )
        extract_parser.add_argument(
            "--processed",
            action="store_true",
            default=True,
            help="Apply data processing and power calculations (default)",
        )
        extract_parser.add_argument(
            "--raw",
            action="store_true",
            help="Export raw data without processing (overrides --processed)",
        )
        extract_parser.add_argument(
            "--no-clean",
            action="store_true",
            help="Disable automatic data cleaning",
        )
        extract_parser.add_argument(
            "--chunk-size",
            type=int,
            default=15,
            help="Chunk size in minutes (default: 15)",
        )
        extract_parser.add_argument(
            "--parallel",
            type=int,
            default=2,
            help="Number of parallel workers (default: 2)",
        )
        extract_parser.add_argument(
            "--diagnostics",
            action="store_true",
            help="Enable detailed performance diagnostics",
        )
        extract_parser.add_argument(
            "--connection-pool",
            type=int,
            default=3,
            help="Connection pool size (default: 3)",
        )
        extract_parser.add_argument(
            "--verbose", "-v", action="store_true", help="Enable verbose logging"
        )
        extract_parser.add_argument(
            "--verbose-timing",
            action="store_true",
            help="Show detailed timing during extraction",
        )
        extract_parser.add_argument(
            "--replace",
            action="store_true",
            help="Replace existing output file (default: skip)",
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
            "--resolution", type=int, default=50, help="Data resolution (default: 50)"
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
            default="csv",
            help="Output format (default: csv)",
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
            help="Disable automatic data cleaning",
        )
        batch_parser.add_argument(
            "--chunk-size",
            type=int,
            default=15,
            help="Chunk size in minutes (default: 15)",
        )
        batch_parser.add_argument(
            "--parallel",
            type=int,
            default=2,
            help="Number of parallel workers (default: 2)",
        )
        batch_parser.add_argument(
            "--connection-pool",
            type=int,
            default=3,
            help="Connection pool size (default: 3)",
        )
        batch_parser.add_argument(
            "--verbose", "-v", action="store_true", help="Enable verbose logging"
        )
        batch_parser.add_argument(
            "--verbose-timing",
            action="store_true",
            help="Show detailed timing during extraction",
        )
        batch_parser.add_argument(
            "--replace",
            action="store_true",
            help="Replace existing output files (default: skip)",
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
