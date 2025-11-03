#!/usr/bin/env python3
"""
PhasorPoint Database CLI Tool
A command-line interface for exploring and extracting data from PhasorPoint database to Parquet files.
Enhanced with data cleaning, smoothing, and validation capabilities.
"""

import contextlib
import logging
import os
import sys
import warnings
from pathlib import Path

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv

    from .config_paths import ConfigPathManager

    # Try to find and load .env file using priority order
    path_manager = ConfigPathManager()
    env_file = path_manager.find_env_file()
    if env_file:
        load_dotenv(dotenv_path=env_file)
    else:
        load_dotenv()  # Fallback to default behavior
except ImportError:
    # dotenv not installed, skip .env file loading
    pass

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    try:
        # Use hasattr to check for reconfigure method (type checker workaround)
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except (AttributeError, Exception):
        # Python < 3.7 or reconfigure not available
        import codecs

        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")

# Suppress pandas SQLAlchemy warning for specialized ODBC drivers
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

# Import modules
from .argument_parser import CLIArgumentParser  # noqa: E402 - placed after environment setup
from .command_router import CommandRouter  # noqa: E402 - placed after environment setup
from .config import ConfigurationManager  # noqa: E402 - placed after environment setup
from .config_paths import ConfigPathManager  # noqa: E402 - placed after environment setup
from .connection_pool import JDBCConnectionPool  # noqa: E402 - placed after environment setup
from .constants import CLI_COMMAND_PYTHON  # noqa: E402 - placed after environment setup


def setup_logging(verbose=False):
    """
    Set up logging configuration.

    Args:
        verbose: If True, also display logs to console. Otherwise logs go to file only.

    Returns:
        Tuple of (logger, log_file_path)
    """
    from datetime import datetime  # noqa: PLC0415

    log_level = logging.INFO if not verbose else logging.DEBUG

    # Get log directory and create timestamped log file
    path_manager = ConfigPathManager()
    log_dir = path_manager.get_log_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"phasor_cli_{timestamp}.log"

    # Create logger
    logger = logging.getLogger("phasor_cli")
    logger.setLevel(log_level)

    # Close existing handlers before clearing to prevent ResourceWarning
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    # Always add file handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Only add stream handler if verbose mode
    if verbose:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(log_level)
        stream_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
        )
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

    # Cleanup old logs (keep last 30 days)
    with contextlib.suppress(Exception):
        path_manager.cleanup_old_logs(days=30)

    return logger, log_file


class PhasorPointCLI:
    """Main CLI class for PhasorPoint database operations"""

    def __init__(  # noqa: PLR0915 - Complex initialization with validation, to be refactored
        self,
        username=None,
        password=None,
        config_file=None,
        connection_pool_size=1,
        logger=None,
        skip_validation=False,
    ):
        # Initialize logger first so it's available for configuration
        self.logger = logger or logging.getLogger("phasor_cli")

        # Use the config path manager if no explicit config provided
        if config_file is None:
            path_manager = ConfigPathManager()
            config_file_path = path_manager.find_config_file()
            config_file = str(config_file_path) if config_file_path else None

        self.config = ConfigurationManager(config_file=config_file, logger=self.logger)

        # For setup command, skip credential validation
        if skip_validation:
            return

        # CRITICAL: NO HARDCODED CREDENTIALS IN SOURCE CODE
        # Priority: CLI args > Environment variables ONLY

        # Check environment variables (required, no fallbacks in source)
        env_username = os.getenv("DB_USERNAME")
        env_password = os.getenv("DB_PASSWORD")
        env_host = os.getenv("DB_HOST")
        env_port = os.getenv("DB_PORT")
        env_database = os.getenv("DB_NAME")

        # Use CLI args or environment variables only
        self.username = username or env_username
        self.password = password or env_password
        self.host = env_host

        # Validate and parse port
        self.port = None
        if env_port:
            try:
                self.port = int(env_port)
                if self.port <= 0 or self.port > 65535:
                    self.logger.error(
                        f"[ERROR] Invalid port number: {env_port}. Must be between 1 and 65535."
                    )
                    print(f"\n[ERROR] Invalid DB_PORT value: '{env_port}'")
                    print("Port must be a number between 1 and 65535")
                    print("\n[FIX] Update your .env file or configuration:")
                    print("   DB_PORT=1433  # or your actual database port")
                    sys.exit(1)
            except ValueError:
                self.logger.error(f"[ERROR] DB_PORT must be a number, got: '{env_port}'")
                print(f"\n[ERROR] Invalid DB_PORT value: '{env_port}'")
                print("Port must be a valid number (e.g., 1433, 5432, 3306)")
                print("\n[FIX] Update your .env file or run:")
                print(f"   {CLI_COMMAND_PYTHON} setup  # to configure database settings")
                sys.exit(1)

        self.database = env_database
        self.driver = "Psymetrix PhasorPoint"  # This is not a credential, it's a driver name

        # Validate credentials - NO HARDCODED FALLBACKS
        missing_creds = []
        if not self.username:
            missing_creds.append("DB_USERNAME")
        if not self.password:
            missing_creds.append("DB_PASSWORD")
        if not self.host:
            missing_creds.append("DB_HOST")
        if not self.port:
            missing_creds.append("DB_PORT")
        if not self.database:
            missing_creds.append("DB_NAME")

        if missing_creds:
            self.logger.error("[ERROR] Missing required database credentials")
            print("\n" + "=" * 70)
            print("ERROR: Missing Database Credentials")
            print("=" * 70)
            print("\nThe following environment variables must be set:")
            for cred in missing_creds:
                print(f"   • {cred}")
            print("\n" + "-" * 70)
            print("SETUP OPTIONS:")
            print("-" * 70)
            print("\n1. Quick Setup (Recommended):")
            print(f"   {CLI_COMMAND_PYTHON} setup")
            print("   → Creates config file with interactive prompts")
            print("\n2. Manual Environment Variables:")
            print("   export DB_USERNAME='your_username'")
            print("   export DB_PASSWORD='your_password'")
            print("   export DB_HOST='your_host'")
            print("   export DB_PORT='1433'")
            print("   export DB_NAME='your_database'")
            print("\n3. Create .env file manually:")
            print("   echo 'DB_USERNAME=your_username' > .env")
            print("   echo 'DB_PASSWORD=your_password' >> .env")
            print("   echo 'DB_HOST=your_host' >> .env")
            print("   echo 'DB_PORT=1433' >> .env")
            print("   echo 'DB_NAME=your_database' >> .env")
            print("\n4. Specify on command line:")
            print(f"   {CLI_COMMAND_PYTHON} --username USER --password PASS [command]")
            print("\n" + "=" * 70)
            print(f"For more info: {CLI_COMMAND_PYTHON} --help")
            print("=" * 70 + "\n")
            sys.exit(1)

        self.connection_string = f"DRIVER={{{self.driver}}};HOST={self.host};PORT={self.port};DATABASE={self.database};UID={self.username};PWD={self.password}"

        # Initialize connection pool
        self.connection_pool = JDBCConnectionPool(
            self.connection_string, connection_pool_size, self.logger
        )
        self.logger.info(f"Initialized connection pool with {connection_pool_size} connections")

    def create_connection(self):
        """Get a connection from the pool"""
        return self.connection_pool.get_connection()

    def cleanup_connections(self):
        """Clean up all connections and pools"""
        self.logger.info("Cleaning up connection pool...")
        try:
            self.connection_pool.cleanup()
            self.logger.info("All connections closed successfully")
        except Exception as e:
            self.logger.warning(f"Error during connection cleanup: {e}")

    def update_connection_pool_size(self, new_size):
        """Update connection pool size (for dynamic reconfiguration)"""
        if new_size != self.connection_pool.max_connections:
            self.logger.info(
                f"Updating connection pool size from {self.connection_pool.max_connections} to {new_size}"
            )
            # Clean up existing pool
            self.connection_pool.cleanup()
            # Create new pool with updated size
            self.connection_pool = JDBCConnectionPool(self.connection_string, new_size, self.logger)


def main():
    """Main entry point for the CLI application."""
    # Build argument parser using new OOP class
    parser = CLIArgumentParser().build()

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Initialize logging first
    logger, log_file = setup_logging(verbose=getattr(args, "verbose", False))

    # Create user output instance
    from .user_output import UserOutput  # noqa: E402, PLC0415

    output = UserOutput(quiet=False)

    # Handle setup, config, about, and aboot commands early (don't need database connection)
    if args.command in ("setup", "config", "about", "aboot"):
        # Create a minimal router without CLI instance for non-DB commands
        # These commands don't require database access, so CLI instance is optional
        router = CommandRouter(None, logger, output)  # type: ignore[arg-type]
        router.route(args.command, args)

        # Show log file location (only if not verbose)
        if not getattr(args, "verbose", False):
            output.blank_line()
            output.info(f"Full diagnostic log saved to: {log_file}", tag="LOG")
        return

    # Determine config file to use (will check multiple locations with priority)
    config_file = args.config
    if config_file is None:
        from .config_paths import ConfigPathManager  # noqa: PLC0415 - late import to avoid overhead

        path_manager = ConfigPathManager()
        config_file_path = path_manager.find_config_file()
        if config_file_path:
            config_file = str(config_file_path)
            logger.info(f"Found and loading config: {config_file}")
        else:
            logger.warning("No configuration file found in standard locations")
            logger.info(
                "Will use embedded defaults with environment variables for database connection"
            )
    elif not Path(config_file).exists():
        # Explicit config provided but doesn't exist
        logger.error(f"[ERROR] Specified config file not found: {config_file}")
        print(f"\n[ERROR] Configuration file not found: {config_file}\n")
        print("[SETUP] To create a configuration file, run:")
        print(f"   {CLI_COMMAND_PYTHON} setup")
        print("\nOr use environment variables for database connection.")
        sys.exit(1)

    # Initialize CLI (for commands that need database access)
    connection_pool_size = getattr(args, "connection_pool", 3)
    cli = PhasorPointCLI(
        args.username,
        args.password,
        config_file=config_file,
        connection_pool_size=connection_pool_size,
        logger=logger,
    )

    # Import and set up signal handling for graceful cancellation
    from .signal_handler import get_cancellation_manager  # noqa: E402, PLC0415

    cancellation_manager = get_cancellation_manager()
    cancellation_manager.set_logger(logger)

    try:
        # Use context manager to register signal handlers
        with cancellation_manager:
            # Dispatch to command router
            router = CommandRouter(cli, logger, output)
            router.route(args.command, args)
    finally:
        # Ensure all connections are properly cleaned up
        cli.cleanup_connections()

        # Show log file location (only if not verbose, since verbose already shows everything)
        if not getattr(args, "verbose", False):
            output.blank_line()
            output.info(f"Full diagnostic log saved to: {log_file}", tag="LOG")


if __name__ == "__main__":
    main()
