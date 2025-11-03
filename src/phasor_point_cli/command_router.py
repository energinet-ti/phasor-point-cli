"""
Command routing for PhasorPoint CLI.

Routes CLI commands to appropriate handlers, orchestrating the flow between
user input and the various manager classes.
"""

import argparse
from datetime import datetime
from typing import TYPE_CHECKING

from .config import ConfigurationManager
from .constants import CLI_COMMAND_PYTHON
from .date_utils import DateRangeCalculator
from .extraction_manager import ExtractionManager
from .models import ExtractionRequest
from .progress_tracker import ScanProgressTracker
from .query_executor import QueryExecutor
from .table_manager import TableManager

if TYPE_CHECKING:
    from .cli import PhasorPointCLI


def _create_scan_progress_callback(tracker: ScanProgressTracker):
    """
    Create a progress callback for table scanning.

    Args:
        tracker: ScanProgressTracker instance to update

    Returns:
        Callback function for progress updates
    """

    def callback(completed: int, total: int, found_count: int) -> None:
        """Progress callback for table scanning."""
        tracker.update(completed, total, found_count)

    return callback


class CommandRouter:
    """Routes CLI commands to appropriate handlers."""

    def __init__(self, cli_instance: "PhasorPointCLI", logger, output=None):
        """
        Initialize command router.

        Args:
            cli_instance: PhasorPointCLI instance to delegate operations to
            logger: Logger instance for logging
            output: UserOutput instance for user-facing messages
        """
        self._cli = cli_instance
        self._logger = logger
        self._output = output
        self._date_calculator = DateRangeCalculator()

    def route(self, command: str, args: argparse.Namespace) -> None:
        """
        Route command to appropriate handler.

        Args:
            command: Command name to route
            args: Parsed command-line arguments

        Raises:
            ValueError: If command is not recognized
        """
        handlers = {
            "setup": self.handle_setup,
            "config": self.handle_config,
            "about": self.handle_about,
            "aboot": self.handle_aboot,
            "list-tables": self.handle_list_tables,
            "table-info": self.handle_table_info,
            "extract": self.handle_extract,
            "batch-extract": self.handle_batch_extract,
            "query": self.handle_query,
        }

        handler = handlers.get(command)
        if handler:
            handler(args)
        else:
            raise ValueError(f"Unknown command: {command}")

    def _check_pmu_in_config(self, pmu_id: int) -> bool:
        """
        Check if a PMU exists in configuration.

        Args:
            pmu_id: PMU ID to check

        Returns:
            True if PMU exists in config, False otherwise
        """
        return self._cli.config.get_pmu_info(pmu_id) is not None

    def _print_pmu_not_in_config_warning(self, pmu_id: int) -> None:
        """Print warning when PMU is not found in configuration."""
        pmu_count = len(self._cli.config.get_all_pmu_ids())

        print(f"\n[WARNING] PMU {pmu_id} not found in configuration")

        if pmu_count == 0:
            print("\n[ROOT CAUSE]")
            print("   • No PMUs loaded in configuration (0 PMUs total)")
            print("\n[SOLUTION]")
            print("   1. Refresh PMU list from database:")
            print(f"      {CLI_COMMAND_PYTHON} setup --refresh-pmus")
            print("\n   This will fetch and load all available PMUs from your database.")
        else:
            print("\n[STATUS]")
            print(f"   • Configuration contains {pmu_count} other PMU(s)")
            print(f"   • PMU {pmu_id} is not in the list")
            print("\n[POSSIBLE CAUSES]")
            print("   • PMU list is outdated")
            print("   • PMU was recently added to database")
            print("   • Incorrect PMU ID")
            print("\n[RECOMMENDED ACTIONS]")
            print(f"   1. Refresh PMU list: {CLI_COMMAND_PYTHON} setup --refresh-pmus")
            print(f"   2. Check available PMUs: {CLI_COMMAND_PYTHON} list-tables")
            print(f"   3. Verify PMU ID {pmu_id} is correct")

        print()

    def _print_no_tables_found_error(self) -> None:
        """Print detailed error message when no PMU tables are found."""
        print("\n" + "=" * 70)
        print("WARNING: No PMU Tables Found")
        print("=" * 70)
        print("\nCould not find any accessible PMU tables in the database.")

        # Check if PMU list is empty in config
        pmu_count = len(self._cli.config.get_all_pmu_ids())

        if pmu_count == 0:
            # PMU list is empty - this is likely the root cause
            print("\n[ROOT CAUSE]")
            print("   • PMU metadata not loaded in configuration (0 PMUs in config)")
            print("\n[SOLUTION]")
            print("   1. Refresh PMU list from database:")
            print(f"      {CLI_COMMAND_PYTHON} setup --refresh-pmus")
            print("\n   This will fetch and load all available PMUs from your database.")
        else:
            # PMU list exists but no tables found - different issue
            print("\n[STATUS]")
            print(f"   • Configuration contains {pmu_count} PMU(s)")
            print("   • But no accessible tables found in database")
            print("\n[POSSIBLE CAUSES]")
            print("   • Database connection issues")
            print("   • PMU list is outdated (PMUs removed from database)")
            print("   • Insufficient database permissions")
            print("   • Wrong database selected")
            print("\n[RECOMMENDED ACTIONS]")
            print("   1. Check connection: Verify DB_HOST, DB_PORT, DB_NAME are correct")
            print(f"   2. Refresh PMU list: {CLI_COMMAND_PYTHON} setup --refresh-pmus")
            print(f"   3. Try specific PMU: {CLI_COMMAND_PYTHON} list-tables --pmu 45020")
            print("   4. Check permissions: Ensure user can read PMU tables")

        print("\n[NEED HELP?]")
        print("   • Verify database connection settings in your .env or config.json")
        print("   • Contact your database administrator if issue persists")
        print("=" * 70 + "\n")

    def handle_setup(self, args: argparse.Namespace) -> None:
        """
        Handle the 'setup' command to create configuration files.

        Args:
            args: Parsed command-line arguments
        """
        ConfigurationManager.setup_configuration_files(
            force=getattr(args, "force", False),
            local=getattr(args, "local", False),
            interactive=getattr(args, "interactive", True),
            refresh_pmus=getattr(args, "refresh_pmus", False),
        )

    def handle_config(self, args: argparse.Namespace) -> None:  # noqa: PLR0912, PLR0915
        """
        Handle the 'config' command to display or manage configuration files.

        Args:
            args: Parsed command-line arguments
        """
        # If --clean flag is set, remove configuration files
        if getattr(args, "clean", False):
            ConfigurationManager.cleanup_configuration_files(
                local=getattr(args, "local", False),
                all_locations=getattr(args, "all", False),
            )
            return

        # Otherwise, display configuration file locations
        import os  # noqa: PLC0415 - avoid importing at module import time

        from .config_paths import ConfigPathManager  # noqa: PLC0415 - late import for CLI perf

        path_manager = ConfigPathManager()
        info = path_manager.get_config_locations_info()

        print("\n" + "=" * 70)
        print("PhasorPoint CLI Configuration Paths")
        print("=" * 70)

        print(f"\nUser Config Directory: {info['user_config_dir']}")

        print("\n" + "-" * 70)
        print("Configuration Files (in priority order):")
        print("-" * 70)

        # Environment variables (highest priority)
        print("\n1. ENVIRONMENT VARIABLES (Highest Priority)")
        env_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USERNAME", "DB_PASSWORD"]
        found_any_env = False
        for var in env_vars:
            value = os.getenv(var)
            if value:
                found_any_env = True
                # Mask password
                display_value = "*" * min(len(value), 8) if "PASSWORD" in var else value
                print(f"   {var}={display_value}")
        if not found_any_env:
            print("   (None set)")

        # Local .env file
        print("\n2. LOCAL .env FILE (Project-specific)")
        local_env = info["local_env"]
        if local_env["exists"]:
            print(f"   [FOUND] {local_env['path']}")
        else:
            print(f"   [NOT FOUND] {local_env['path']}")

        # Local config.json
        print("\n3. LOCAL config.json (Project-specific)")
        local_config = info["local_config"]
        if local_config["exists"]:
            print(f"   [FOUND] {local_config['path']}")
        else:
            print(f"   [NOT FOUND] {local_config['path']}")

        # User .env file
        print("\n4. USER .env FILE (Global)")
        user_env = info["user_env"]
        if user_env["exists"]:
            print(f"   [FOUND] {user_env['path']}")
        else:
            print(f"   [NOT FOUND] {user_env['path']}")

        # User config.json
        print("\n5. USER config.json (Global)")
        user_config = info["user_config"]
        if user_config["exists"]:
            print(f"   [FOUND] {user_config['path']}")
        else:
            print(f"   [NOT FOUND] {user_config['path']}")

        # Embedded defaults
        print("\n6. EMBEDDED DEFAULTS (Lowest Priority)")
        print("   [ALWAYS AVAILABLE] Built-in configuration")

        print("\n" + "-" * 70)
        print("Currently Active Configuration:")
        print("-" * 70)

        active_env = info["active_env"]
        active_config = info["active_config"]

        if active_env:
            print(f"   .env:        {active_env}")
        else:
            print("   .env:        (Using environment variables or none)")

        if active_config:
            print(f"   config.json: {active_config}")
        else:
            print("   config.json: (Using embedded defaults)")

        print("\n" + "-" * 70)
        print("Management Commands:")
        print("-" * 70)
        print(
            f"   {CLI_COMMAND_PYTHON} setup               # Create user-level config (recommended)"
        )
        print(f"   {CLI_COMMAND_PYTHON} setup --local       # Create project-specific config")
        print(f"   {CLI_COMMAND_PYTHON} config --clean      # Remove configuration files")
        print("\n")

    def handle_about(self, _args: argparse.Namespace) -> None:
        """
        Handle the 'about' command to display version and about information.

        Args:
            args: Parsed command-line arguments
        """
        from .banner import print_about  # noqa: PLC0415 - late import for CLI perf

        print_about()

    def handle_aboot(self, _args: argparse.Namespace) -> None:
        """
        Handle the hidden 'aboot' command (easter egg).

        Args:
            args: Parsed command-line arguments
        """
        from .banner import print_pirate_raccoon  # noqa: PLC0415 - late import for CLI perf

        print_pirate_raccoon()

    def handle_list_tables(self, args: argparse.Namespace) -> None:
        """
        Handle the 'list-tables' command to list available PMU tables.

        Args:
            args: Parsed command-line arguments
        """
        pmu_ids = getattr(args, "pmu", None)
        max_pmus = None if getattr(args, "all", False) else getattr(args, "max_pmus", 10)
        resolutions = None  # Use default resolutions

        # Create and start scan progress tracker
        scan_tracker = ScanProgressTracker()
        scan_tracker.start()

        manager = TableManager(self._cli.connection_pool, self._cli.config, self._logger)

        try:
            result = manager.list_available_tables(
                pmu_ids=pmu_ids,
                resolutions=resolutions,
                max_pmus=max_pmus,
                progress_callback=_create_scan_progress_callback(scan_tracker),
            )

            # Finish scan progress display
            scan_tracker.finish()
        except Exception:
            # Stop tracker on error
            scan_tracker.stop()
            raise

        if not result or not result.found_pmus:
            self._logger.error("No accessible PMU tables found")
            self._print_no_tables_found_error()
            return

        # Display results
        self._logger.info(
            f"Found {len(result.found_pmus)} PMUs with {result.total_tables} accessible tables"
        )
        print("=" * 100)
        print(f"{'PMU':<8} {'Name':<30} {'Resolutions':<15} {'Tables'}")
        print("=" * 100)

        unknown_pmus = []
        for pmu in sorted(result.found_pmus.keys()):
            resolutions_list = sorted(result.found_pmus[pmu])
            pmu_info = self._cli.config.get_pmu_info(pmu)
            if pmu_info:
                name_str = pmu_info.station_name
                if pmu_info.country:
                    name_str = f"{name_str} ({pmu_info.country})"
            else:
                name_str = "Unknown"
                unknown_pmus.append(pmu)

            res_str = ", ".join(map(str, resolutions_list))
            tables_str = ", ".join([f"pmu_{pmu}_{r}" for r in resolutions_list])
            print(f"{pmu:<8} {name_str:<30} {res_str:<15} {tables_str}")

        print("=" * 100)

        # Warn if any PMUs show as Unknown
        if unknown_pmus:
            print(
                f"\n[NOTE] {len(unknown_pmus)} PMU(s) show as 'Unknown' - metadata not in configuration"
            )
            print(f"   To get PMU names: {CLI_COMMAND_PYTHON} setup --refresh-pmus")

        if result.found_pmus:
            example_pmu = sorted(result.found_pmus.keys())[0]
            print("\n" + "-" * 100)
            print("Next Steps:")
            print("-" * 100)
            print(f"  View details:  {CLI_COMMAND_PYTHON} table-info --pmu {example_pmu}")
            print(f"  Extract data:  {CLI_COMMAND_PYTHON} extract --pmu {example_pmu} --hours 1")
            print("-" * 100)

    def handle_table_info(self, args: argparse.Namespace) -> None:
        """
        Handle the 'table-info' command to display table information.

        Args:
            args: Parsed command-line arguments
        """
        manager = TableManager(self._cli.connection_pool, self._cli.config, self._logger)
        table_info = manager.get_table_info(args.pmu, args.resolution)

        if not table_info:
            self._logger.error(
                f"Table pmu_{args.pmu}_{args.resolution} does not exist or is not accessible"
            )
            print(f"\n[ERROR] Table pmu_{args.pmu}_{args.resolution} not found or not accessible")
            print("\n[POSSIBLE CAUSES]")
            print("   • PMU metadata not refreshed - PMU list may be outdated")
            print("   • PMU does not exist in database")
            print("   • Insufficient permissions to access this table")
            print("\n[RECOMMENDED ACTIONS]")
            print(f"   1. Refresh PMU list: {CLI_COMMAND_PYTHON} setup --refresh-pmus")
            print(f"   2. List available PMUs: {CLI_COMMAND_PYTHON} list-tables")
            print("   3. Check different resolution if PMU exists")
            return

        # Display PMU info
        if table_info.pmu_info:
            name = table_info.pmu_info.station_name
            country = table_info.pmu_info.country
            if country:
                self._logger.info(
                    "Inspecting %s for PMU %s (%s, %s)",
                    table_info.table_name,
                    args.pmu,
                    name,
                    country,
                )
                print(f"[PMU] {args.pmu} - {name} ({country})")
            else:
                self._logger.info(
                    "Inspecting %s for PMU %s (%s)", table_info.table_name, args.pmu, name
                )
                print(f"[PMU] {args.pmu} - {name}")

        # Display table statistics
        print("=" * 80)
        print(
            f"PMU {args.pmu} ({table_info.pmu_info.station_name if table_info.pmu_info else 'Unknown'}) - Resolution: {args.resolution} Hz"
        )
        print(f"Table name: {table_info.table_name}")
        # Show "Unknown" for row count if 0 (custom JDBC doesn't support COUNT)
        row_display = (
            "Unknown (COUNT not supported by database)"
            if table_info.statistics.row_count == 0
            else f"{table_info.statistics.row_count:,}"
        )
        print(f"Rows: {row_display}")
        print(f"Columns: {table_info.statistics.column_count}")
        if table_info.statistics.start_time and table_info.statistics.end_time:
            print(
                f"Time range: {table_info.statistics.start_time} to {table_info.statistics.end_time}"
            )
        elif table_info.statistics.start_time:
            print(f"Earliest timestamp: {table_info.statistics.start_time}")
        print("=" * 80)

        # Show next steps after table info
        print("\n" + "-" * 80)
        print("Next Step - Extract Data:")
        print("-" * 80)
        print(f"  Last hour:  {CLI_COMMAND_PYTHON} extract --pmu {args.pmu} --hours 1")
        if table_info.statistics.start_time:
            print(f"  Last day:   {CLI_COMMAND_PYTHON} extract --pmu {args.pmu} --days 1")
        print("-" * 80)

        # Display sample data
        if table_info.sample_data is not None and not table_info.sample_data.empty:
            print("\n" + "=" * 80)
            print(f"SAMPLE DATA - {table_info.table_name} (first 5 rows)")
            print("=" * 80)
            print(table_info.sample_data.head(5).to_string(index=False))
            print("=" * 80)
        else:
            print("[INFO] No sample data available for table")

    def handle_extract(self, args: argparse.Namespace) -> None:
        """
        Handle the 'extract' command to extract data from a single PMU.

        Args:
            args: Parsed command-line arguments
        """
        # Check if PMU exists in configuration
        if not self._check_pmu_in_config(args.pmu):
            self._logger.warning(f"PMU {args.pmu} not found in configuration")
            self._print_pmu_not_in_config_warning(args.pmu)
            print("[NOTE] Extraction will continue but may fail if PMU doesn't exist in database\n")

        # Capture reference timestamp at command issue time for relative windows
        reference_time = datetime.now()

        try:
            date_range = self._date_calculator.calculate(args, reference_time)
        except ValueError as e:
            self._logger.error(str(e))
            return

        request = ExtractionRequest(
            pmu_id=args.pmu,
            date_range=date_range,
            output_file=args.output,
            resolution=args.resolution,
            processed=args.processed and not args.raw,
            clean=not args.no_clean and not args.raw,
            chunk_size_minutes=args.chunk_size,
            parallel_workers=args.parallel,
            output_format=args.format,
            replace=getattr(args, "replace", False),
        )

        if (
            args.connection_pool
            and args.connection_pool != self._cli.connection_pool.max_connections
        ):
            self._cli.update_connection_pool_size(args.connection_pool)

        verbose_timing = getattr(args, "verbose_timing", False)
        manager = ExtractionManager(
            self._cli.connection_pool,
            self._cli.config,
            self._logger,
            output=self._output,
            verbose_timing=verbose_timing,
        )
        result = manager.extract(request)

        if result.success:
            self._logger.info("Extraction completed: %s", result.output_file)
        else:
            self._logger.error("Extraction failed: %s", result.error)

    def handle_batch_extract(self, args: argparse.Namespace) -> None:
        """
        Handle the 'batch-extract' command to extract data from multiple PMUs.

        Args:
            args: Parsed command-line arguments
        """
        # Parse PMU IDs from comma-separated string
        pmu_ids = [int(p.strip()) for p in args.pmus.split(",")]

        # Check which PMUs are not in configuration
        missing_pmus = [pmu_id for pmu_id in pmu_ids if not self._check_pmu_in_config(pmu_id)]
        if missing_pmus:
            pmu_count = len(self._cli.config.get_all_pmu_ids())
            self._logger.warning(
                f"{len(missing_pmus)} PMU(s) not found in configuration: {missing_pmus}"
            )
            print(
                f"\n[WARNING] {len(missing_pmus)} of {len(pmu_ids)} PMU(s) not found in configuration:"
            )
            print(f"   Missing: {', '.join(map(str, missing_pmus))}")

            if pmu_count == 0:
                print("\n[ROOT CAUSE]")
                print("   • No PMUs loaded in configuration")
                print("\n[SOLUTION]")
                print(f"   Refresh PMU list: {CLI_COMMAND_PYTHON} setup --refresh-pmus")
                print()
                return

            print("\n[STATUS]")
            print(f"   • Configuration has {pmu_count} PMU(s) but not these {len(missing_pmus)}")
            print("\n[RECOMMENDED ACTION]")
            print(f"   Refresh PMU list: {CLI_COMMAND_PYTHON} setup --refresh-pmus")
            print(
                "\n[NOTE] Batch extraction will continue with all PMUs but may fail for missing ones\n"
            )

        # Capture reference timestamp at command issue time for consistent batch extraction
        reference_time = datetime.now()

        try:
            date_range = self._date_calculator.calculate(args, reference_time)
        except ValueError as e:
            self._logger.error(str(e))
            return

        # Create extraction requests for each PMU
        from pathlib import Path  # noqa: PLC0415 - minimize module import overhead

        requests = []
        for pmu_id in pmu_ids:
            request = ExtractionRequest(
                pmu_id=pmu_id,
                date_range=date_range,
                output_file=None,  # Will be auto-generated by ExtractionManager
                resolution=args.resolution,
                processed=args.processed and not args.raw,
                clean=not args.no_clean and not args.raw,
                chunk_size_minutes=args.chunk_size,
                parallel_workers=args.parallel,
                output_format=args.format,
                replace=getattr(args, "replace", False),
            )
            requests.append(request)

        # Update connection pool size if needed
        if (
            args.connection_pool
            and args.connection_pool != self._cli.connection_pool.max_connections
        ):
            self._cli.update_connection_pool_size(args.connection_pool)

        # Execute batch extraction
        output_dir = Path(args.output_dir) if args.output_dir else None
        verbose_timing = getattr(args, "verbose_timing", False)
        manager = ExtractionManager(
            self._cli.connection_pool,
            self._cli.config,
            self._logger,
            output=self._output,
            verbose_timing=verbose_timing,
        )
        batch_result = manager.batch_extract(requests, output_dir=output_dir)

        # Calculate summary stats
        total = len(batch_result.results)
        successful_count = len(batch_result.successful_results())
        failed_count = len(batch_result.failed_results())

        # Always log to technical logger
        self._logger.info(
            f"Batch extraction completed: {successful_count}/{total} successful, {failed_count}/{total} failed"
        )

    def handle_query(self, args: argparse.Namespace) -> None:
        """
        Handle the 'query' command to execute a custom SQL query.

        Args:
            args: Parsed command-line arguments
        """
        executor = QueryExecutor(self._cli.connection_pool, self._logger)
        result = executor.execute(args.sql, output_file=args.output, output_format=args.format)
        if not result.success and result.error:
            self._logger.error("Query execution failed: %s", result.error)
