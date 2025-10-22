"""
Command routing for PhasorPoint CLI.

Routes CLI commands to appropriate handlers, orchestrating the flow between
user input and the various manager classes.
"""

import argparse
from datetime import datetime
from typing import TYPE_CHECKING

from .config import cleanup_configuration, setup_configuration
from .date_utils import DateRangeCalculator
from .extraction_manager import ExtractionManager
from .models import ExtractionRequest
from .query_executor import QueryExecutor
from .table_manager import TableManager

if TYPE_CHECKING:
    from .cli import PhasorPointCLI


class CommandRouter:
    """Routes CLI commands to appropriate handlers."""

    def __init__(self, cli_instance: "PhasorPointCLI", logger):
        """
        Initialize command router.

        Args:
            cli_instance: PhasorPointCLI instance to delegate operations to
            logger: Logger instance for logging
        """
        self._cli = cli_instance
        self._logger = logger
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
            "config-path": self.handle_config_path,
            "config-clean": self.handle_config_clean,
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

    def handle_setup(self, args: argparse.Namespace) -> None:
        """
        Handle the 'setup' command to create configuration files.

        Args:
            args: Parsed command-line arguments
        """
        setup_configuration(
            force=args.force if hasattr(args, "force") else False,
            local=args.local if hasattr(args, "local") else False,
            interactive=args.interactive if hasattr(args, "interactive") else False,
            refresh_pmus=args.refresh_pmus if hasattr(args, "refresh_pmus") else False,
        )

    def handle_config_path(self, _args: argparse.Namespace) -> None:  # noqa: PLR0912, PLR0915
        """
        Handle the 'config-path' command to display configuration file locations.

        Args:
            args: Parsed command-line arguments
        """
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
        print("Setup Commands:")
        print("-" * 70)
        print("   phasor-cli setup               # Create user-level config (recommended)")
        print("   phasor-cli setup --local       # Create project-specific config")
        print("   phasor-cli setup --force       # Overwrite existing config files")
        print("   phasor-cli setup --interactive # Interactive credential entry")
        print("\n")

    def handle_config_clean(self, args: argparse.Namespace) -> None:
        """
        Handle the 'config-clean' command to remove configuration files.

        Args:
            args: Parsed command-line arguments
        """
        cleanup_configuration(
            local=args.local if hasattr(args, "local") else False,
            all_locations=args.all if hasattr(args, "all") else False,
        )

    def handle_list_tables(self, args: argparse.Namespace) -> None:
        """
        Handle the 'list-tables' command to list available PMU tables.

        Args:
            args: Parsed command-line arguments
        """
        pmu_ids = args.pmu if hasattr(args, "pmu") and args.pmu else None
        max_pmus = (
            None
            if (hasattr(args, "all") and args.all)
            else (args.max_pmus if hasattr(args, "max_pmus") else 10)
        )
        resolutions = None  # Use default resolutions

        manager = TableManager(self._cli.connection_pool, self._cli.config, self._logger)
        result = manager.list_available_tables(
            pmu_ids=pmu_ids, resolutions=resolutions, max_pmus=max_pmus
        )

        if not result or not result.found_pmus:
            self._logger.error("No accessible PMU tables found")
            print("\n" + "=" * 70)
            print("WARNING: No PMU Tables Found")
            print("=" * 70)
            print("\nCould not find any accessible PMU tables in the database.")
            print("\n[POSSIBLE CAUSES]")
            print("   • Database connection issues")
            print("   • No PMUs configured in the database")
            print("   • Insufficient database permissions")
            print("   • Wrong database selected")
            print("\n[TROUBLESHOOTING]")
            print("   1. Check connection: Verify DB_HOST, DB_PORT, DB_NAME are correct")
            print("   2. Try specific PMU: phasor-cli list-tables --pmu 45020")
            print("   3. Check permissions: Ensure user can read PMU tables")
            print("   4. Run setup: phasor-cli setup --refresh-pmus")
            print("\n[NEXT STEPS]")
            print("   • Verify database connection settings in your .env or config.json")
            print("   • Contact your database administrator if issue persists")
            print("=" * 70 + "\n")
            return

        # Display results
        self._logger.info(
            f"Found {len(result.found_pmus)} PMUs with {result.total_tables} accessible tables"
        )
        print("=" * 100)
        print(f"{'PMU':<8} {'Name':<25} {'Region':<20} {'Resolutions':<15} {'Tables'}")
        print("=" * 100)

        for pmu in sorted(result.found_pmus.keys()):
            resolutions_list = sorted(result.found_pmus[pmu])
            pmu_info = self._cli.config.get_pmu_info(pmu)
            if pmu_info:
                name_str = pmu_info.station_name
                if pmu_info.country:
                    name_str = f"{name_str} ({pmu_info.country})"
                region = pmu_info.region
            else:
                name_str = "Unknown"
                region = "Unknown"

            res_str = ", ".join(map(str, resolutions_list))
            tables_str = ", ".join([f"pmu_{pmu}_{r}" for r in resolutions_list])
            print(f"{pmu:<8} {name_str:<25} {region:<20} {res_str:<15} {tables_str}")

        print("=" * 100)
        if result.found_pmus:
            example_pmu = sorted(result.found_pmus.keys())[0]
            example_res = result.found_pmus[example_pmu][0]
            print(
                f"\n[TIP] Use 'phasor-cli table-info --pmu {example_pmu} --resolution {example_res}' for detailed information"
            )

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
            print("[TIP] Use 'phasor-cli list-tables' to see available PMUs")
            return

        # Display PMU info
        if table_info.pmu_info:
            name = table_info.pmu_info.station_name
            country = table_info.pmu_info.country
            region = table_info.pmu_info.region
            if country:
                self._logger.info(
                    "Inspecting %s for PMU %s (%s, %s)",
                    table_info.table_name,
                    args.pmu,
                    name,
                    country,
                )
                print(f"[PMU] {args.pmu} - {name} ({country}) [{region}]")
            else:
                self._logger.info(
                    "Inspecting %s for PMU %s (%s)", table_info.table_name, args.pmu, name
                )
                print(f"[PMU] {args.pmu} - {name} [{region}]")

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
            clean=not args.no_clean,
            chunk_size_minutes=args.chunk_size,
            parallel_workers=args.parallel,
            output_format=args.format,
            skip_existing=args.skip_existing if hasattr(args, "skip_existing") else True,
            replace=args.replace if hasattr(args, "replace") else False,
        )

        if (
            args.connection_pool
            and args.connection_pool != self._cli.connection_pool.max_connections
        ):
            self._cli.update_connection_pool_size(args.connection_pool)

        manager = ExtractionManager(self._cli.connection_pool, self._cli.config, self._logger)
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
                clean=not args.no_clean,
                chunk_size_minutes=args.chunk_size,
                parallel_workers=args.parallel,
                output_format=args.format,
                skip_existing=args.skip_existing if hasattr(args, "skip_existing") else True,
                replace=args.replace if hasattr(args, "replace") else False,
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
        manager = ExtractionManager(self._cli.connection_pool, self._cli.config, self._logger)
        batch_result = manager.batch_extract(requests, output_dir=output_dir)

        # Display summary
        print("\n" + "=" * 60)
        print("[DATA] Batch Extraction Summary")
        print("=" * 60)
        total = len(batch_result.results)
        successful_count = len(batch_result.successful_results())
        failed_count = len(batch_result.failed_results())
        self._logger.info(
            f"Batch extraction completed: {successful_count}/{total} successful, {failed_count}/{total} failed"
        )

        successful = batch_result.successful_results()
        if successful:
            self._logger.info("Successfully extracted:")
            for result in successful:
                pmu_id = result.request.pmu_id
                print(f"   PMU {pmu_id}: {result.output_file}")

        failed = batch_result.failed_results()
        if failed:
            self._logger.error("Failed extractions:")
            for result in failed:
                pmu_id = result.request.pmu_id
                print(f"   PMU {pmu_id}: {result.error}")

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
