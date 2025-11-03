"""
Unit tests for CLI module.

Tests the main CLI entry point, initialization logic, command routing,
and error handling using the AAA (Arrange-Act-Assert) pattern.
"""

import argparse
import logging
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import ANY, Mock, patch

import pytest

from phasor_point_cli.cli import PhasorPointCLI, main, setup_logging
from phasor_point_cli.constants import CLI_COMMAND_PYTHON


class TestSetupLogging:
    """Test suite for setup_logging function."""

    @patch("phasor_point_cli.cli.ConfigPathManager")
    def test_setup_logging_default_level(self, mock_path_manager_class, tmp_path):
        """Test setup_logging with default verbosity (INFO level)."""
        # Arrange
        verbose = False
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        mock_path_manager = Mock()
        mock_path_manager.get_log_dir.return_value = log_dir
        mock_path_manager.cleanup_old_logs.return_value = None
        mock_path_manager_class.return_value = mock_path_manager

        # Act
        logger, log_file = setup_logging(verbose=verbose)

        # Assert
        assert logger is not None
        assert logger.name == "phasor_cli"
        assert log_file is not None

    @patch("phasor_point_cli.cli.ConfigPathManager")
    def test_setup_logging_verbose_level(self, mock_path_manager_class, tmp_path):
        """Test setup_logging with verbose flag (DEBUG level)."""
        # Arrange
        verbose = True
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        mock_path_manager = Mock()
        mock_path_manager.get_log_dir.return_value = log_dir
        mock_path_manager.cleanup_old_logs.return_value = None
        mock_path_manager_class.return_value = mock_path_manager

        # Act
        logger, log_file = setup_logging(verbose=verbose)

        # Assert
        assert logger is not None
        assert logger.name == "phasor_cli"
        assert log_file is not None
        assert logger.level == logging.DEBUG

    @patch("phasor_point_cli.cli.ConfigPathManager")
    def test_setup_logging_returns_logger_instance(self, mock_path_manager_class, tmp_path):
        """Test that setup_logging returns a logger instance."""
        # Arrange
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        mock_path_manager = Mock()
        mock_path_manager.get_log_dir.return_value = log_dir
        mock_path_manager.cleanup_old_logs.return_value = None
        mock_path_manager_class.return_value = mock_path_manager

        # Act
        logger, log_file = setup_logging()

        # Assert
        assert isinstance(logger, logging.Logger)
        assert log_file is not None


class TestPhasorPointCLI:
    """Test suite for PhasorPointCLI class."""

    @pytest.fixture
    def mock_logger(self):
        """Create a mock logger."""
        return Mock(spec=logging.Logger)

    @pytest.fixture
    def mock_connection_pool(self):
        """Create a mock connection pool."""
        pool = Mock()
        pool.max_connections = 3
        return pool

    @pytest.fixture
    def valid_env_vars(self, monkeypatch):
        """Set up valid environment variables."""
        # Arrange
        monkeypatch.setenv("DB_HOST", "test_host")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_NAME", "test_db")
        monkeypatch.setenv("DB_USERNAME", "test_user")
        monkeypatch.setenv("DB_PASSWORD", "test_pass")

    def test_initialization_with_skip_validation(self, mock_logger):
        """Test PhasorPointCLI initialization with skip_validation=True."""
        # Arrange
        skip_validation = True

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            cli = PhasorPointCLI(skip_validation=skip_validation, logger=mock_logger)

        # Assert
        assert cli is not None
        assert cli.logger == mock_logger

    def test_initialization_with_valid_credentials(self, mock_logger, valid_env_vars):
        """Test PhasorPointCLI initialization with valid environment credentials."""
        # Arrange
        username = "test_user"
        password = "test_pass"

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with patch("phasor_point_cli.cli.JDBCConnectionPool") as mock_pool:
                cli = PhasorPointCLI(username=username, password=password, logger=mock_logger)

        # Assert
        assert cli.username == username
        assert cli.password == password
        assert cli.host == "test_host"
        assert cli.port == 5432
        assert cli.database == "test_db"
        mock_pool.assert_called_once()

    def test_initialization_missing_username(self, mock_logger, monkeypatch):
        """Test PhasorPointCLI initialization fails with missing username."""
        # Arrange
        monkeypatch.delenv("DB_USERNAME", raising=False)  # Ensure it's not set
        monkeypatch.setenv("DB_HOST", "test_host")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_NAME", "test_db")
        monkeypatch.setenv("DB_PASSWORD", "test_pass")

        # Act & Assert
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with pytest.raises(SystemExit) as exc_info:
                PhasorPointCLI(logger=mock_logger)

            assert exc_info.value.code == 1

    def test_initialization_missing_password(self, mock_logger, monkeypatch):
        """Test PhasorPointCLI initialization fails with missing password."""
        # Arrange
        monkeypatch.delenv("DB_PASSWORD", raising=False)  # Ensure it's not set
        monkeypatch.setenv("DB_HOST", "test_host")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_NAME", "test_db")
        monkeypatch.setenv("DB_USERNAME", "test_user")

        # Act & Assert
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with pytest.raises(SystemExit) as exc_info:
                PhasorPointCLI(logger=mock_logger)

            assert exc_info.value.code == 1

    def test_initialization_missing_host(self, mock_logger, monkeypatch):
        """Test PhasorPointCLI initialization fails with missing host."""
        # Arrange
        monkeypatch.delenv("DB_HOST", raising=False)  # Ensure it's not set
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_NAME", "test_db")
        monkeypatch.setenv("DB_USERNAME", "test_user")
        monkeypatch.setenv("DB_PASSWORD", "test_pass")

        # Act & Assert
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with pytest.raises(SystemExit) as exc_info:
                PhasorPointCLI(logger=mock_logger)

            assert exc_info.value.code == 1

    def test_initialization_missing_all_credentials(self, mock_logger, monkeypatch):
        """Test PhasorPointCLI initialization fails with all credentials missing."""
        # Arrange - clear all environment variables
        monkeypatch.delenv("DB_USERNAME", raising=False)
        monkeypatch.delenv("DB_PASSWORD", raising=False)
        monkeypatch.delenv("DB_HOST", raising=False)
        monkeypatch.delenv("DB_PORT", raising=False)
        monkeypatch.delenv("DB_NAME", raising=False)

        # Act & Assert
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with pytest.raises(SystemExit) as exc_info:
                PhasorPointCLI(logger=mock_logger)

            assert exc_info.value.code == 1
            mock_logger.error.assert_called_once()

    def test_initialization_cli_args_override_env(self, mock_logger, valid_env_vars):
        """Test that CLI arguments override environment variables."""
        # Arrange
        cli_username = "cli_user"
        cli_password = "cli_pass"

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with patch("phasor_point_cli.cli.JDBCConnectionPool"):
                cli = PhasorPointCLI(
                    username=cli_username, password=cli_password, logger=mock_logger
                )

        # Assert
        assert cli.username == cli_username
        assert cli.password == cli_password

    def test_initialization_creates_connection_string(self, mock_logger, valid_env_vars):
        """Test that initialization creates proper connection string."""
        # Arrange
        username = "test_user"
        password = "test_pass"

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with patch("phasor_point_cli.cli.JDBCConnectionPool"):
                cli = PhasorPointCLI(username=username, password=password, logger=mock_logger)

        # Assert
        expected_string = "DRIVER={Psymetrix PhasorPoint};HOST=test_host;PORT=5432;DATABASE=test_db;UID=test_user;PWD=test_pass"
        assert cli.connection_string == expected_string

    def test_initialization_with_custom_pool_size(self, mock_logger, valid_env_vars):
        """Test initialization with custom connection pool size."""
        # Arrange
        pool_size = 5

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with patch("phasor_point_cli.cli.JDBCConnectionPool") as mock_pool:
                PhasorPointCLI(
                    username="test_user",
                    password="test_pass",
                    connection_pool_size=pool_size,
                    logger=mock_logger,
                )

        # Assert
        mock_pool.assert_called_once()
        call_args = mock_pool.call_args
        assert call_args[0][1] == pool_size  # Second argument is pool size

    def test_initialization_with_config_file(self, mock_logger, valid_env_vars):
        """Test initialization with custom config file."""
        # Arrange
        config_file = "custom_config.json"

        # Act
        with ExitStack() as stack:
            mock_config_manager = stack.enter_context(
                patch("phasor_point_cli.cli.ConfigurationManager")
            )
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            stack.enter_context(patch("phasor_point_cli.cli.JDBCConnectionPool"))
            cli = PhasorPointCLI(
                username="test_user",
                password="test_pass",
                config_file=config_file,
                logger=mock_logger,
            )

        # Assert
        mock_config_manager.assert_called_once_with(config_file=config_file, logger=mock_logger)
        assert cli.config is not None

    def test_create_connection(self, mock_logger, valid_env_vars):
        """Test create_connection method."""
        # Arrange
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with patch("phasor_point_cli.cli.JDBCConnectionPool") as mock_pool_class:
                mock_pool = Mock()
                mock_connection = Mock()
                mock_pool.get_connection.return_value = mock_connection
                mock_pool_class.return_value = mock_pool

                cli = PhasorPointCLI(username="test_user", password="test_pass", logger=mock_logger)

        # Act
        connection = cli.create_connection()

        # Assert
        assert connection == mock_connection
        mock_pool.get_connection.assert_called_once()

    def test_cleanup_connections_success(self, mock_logger, valid_env_vars):
        """Test cleanup_connections method with successful cleanup."""
        # Arrange
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with patch("phasor_point_cli.cli.JDBCConnectionPool") as mock_pool_class:
                mock_pool = Mock()
                mock_pool_class.return_value = mock_pool

                cli = PhasorPointCLI(username="test_user", password="test_pass", logger=mock_logger)

        # Act
        cli.cleanup_connections()

        # Assert
        mock_pool.cleanup.assert_called_once()
        assert mock_logger.info.call_count >= 2  # Called for cleanup start and success

    def test_cleanup_connections_with_error(self, mock_logger, valid_env_vars):
        """Test cleanup_connections method handles errors gracefully."""
        # Arrange
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with patch("phasor_point_cli.cli.JDBCConnectionPool") as mock_pool_class:
                mock_pool = Mock()
                mock_pool.cleanup.side_effect = Exception("Cleanup error")
                mock_pool_class.return_value = mock_pool

                cli = PhasorPointCLI(username="test_user", password="test_pass", logger=mock_logger)

        # Act
        cli.cleanup_connections()

        # Assert
        mock_pool.cleanup.assert_called_once()
        mock_logger.warning.assert_called_once()

    def test_update_connection_pool_size_creates_new_pool(self, mock_logger, valid_env_vars):
        """Test update_connection_pool_size creates a new pool when size differs."""
        # Arrange
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with patch("phasor_point_cli.cli.JDBCConnectionPool") as mock_pool_class:
                mock_pool = Mock()
                mock_pool.max_connections = 3
                mock_pool_class.return_value = mock_pool

                cli = PhasorPointCLI(
                    username="test_user",
                    password="test_pass",
                    connection_pool_size=3,
                    logger=mock_logger,
                )

                # Act
                cli.update_connection_pool_size(5)

                # Assert
                mock_pool.cleanup.assert_called_once()
                assert mock_pool_class.call_count == 2  # Initial + update

    def test_update_connection_pool_size_skips_if_same(self, mock_logger, valid_env_vars):
        """Test update_connection_pool_size skips update if size is the same."""
        # Arrange
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with patch("phasor_point_cli.cli.JDBCConnectionPool") as mock_pool_class:
                mock_pool = Mock()
                mock_pool.max_connections = 3
                mock_pool_class.return_value = mock_pool

                cli = PhasorPointCLI(
                    username="test_user",
                    password="test_pass",
                    connection_pool_size=3,
                    logger=mock_logger,
                )

                initial_call_count = mock_pool_class.call_count

        # Act
        cli.update_connection_pool_size(3)

        # Assert
        assert mock_pool_class.call_count == initial_call_count  # No new pool created
        mock_pool.cleanup.assert_not_called()


class TestMainFunction:
    """Test suite for main() function."""

    @pytest.fixture
    def mock_parser_args(self):
        """Create mock argument parser args."""
        args = Mock()
        args.command = "list-tables"
        args.verbose = False
        args.config = None
        args.username = "test_user"
        args.password = "test_pass"
        args.connection_pool = 3
        return args

    def test_main_displays_help_when_no_command(self):
        """Test main() displays help when no command is provided."""
        # Arrange
        test_args = [CLI_COMMAND_PYTHON]

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("sys.argv", test_args))
            mock_parser_class = stack.enter_context(patch("phasor_point_cli.cli.CLIArgumentParser"))
            mock_parser = Mock()
            mock_parser_instance = Mock()
            mock_parser_instance.parse_args.return_value = argparse.Namespace(command=None)
            mock_parser.build.return_value = mock_parser_instance
            mock_parser_class.return_value = mock_parser
            main()

        # Assert
        mock_parser_instance.print_help.assert_called_once()

    def test_main_handles_setup_command_without_db_connection(self):
        """Test main() handles setup command without database connection."""
        # Arrange
        test_args = [CLI_COMMAND_PYTHON, "setup"]

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("sys.argv", test_args))
            mock_parser_class = stack.enter_context(patch("phasor_point_cli.cli.CLIArgumentParser"))
            mock_router_class = stack.enter_context(patch("phasor_point_cli.cli.CommandRouter"))
            mock_logging = stack.enter_context(patch("phasor_point_cli.cli.setup_logging"))

            mock_logger = Mock()
            mock_log_file = Mock()
            mock_logging.return_value = (mock_logger, mock_log_file)

            mock_parser = Mock()
            mock_parser_instance = Mock()
            mock_args = argparse.Namespace(command="setup", verbose=False)
            mock_parser_instance.parse_args.return_value = mock_args
            mock_parser.build.return_value = mock_parser_instance
            mock_parser_class.return_value = mock_parser

            mock_router = Mock()
            mock_router_class.return_value = mock_router

            main()

        # Assert
        mock_router_class.assert_called_once_with(None, mock_logger, ANY)
        mock_router.route.assert_called_once_with("setup", mock_args)

    def test_main_handles_config_command_without_db_connection(self):
        """Test main() handles config command without database connection."""
        # Arrange
        test_args = [CLI_COMMAND_PYTHON, "config"]

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("sys.argv", test_args))
            mock_parser_class = stack.enter_context(patch("phasor_point_cli.cli.CLIArgumentParser"))
            mock_router_class = stack.enter_context(patch("phasor_point_cli.cli.CommandRouter"))
            mock_logging = stack.enter_context(patch("phasor_point_cli.cli.setup_logging"))

            mock_logger = Mock()
            mock_log_file = Mock()
            mock_logging.return_value = (mock_logger, mock_log_file)

            mock_parser = Mock()
            mock_parser_instance = Mock()
            mock_args = argparse.Namespace(command="config", verbose=False)
            mock_parser_instance.parse_args.return_value = mock_args
            mock_parser.build.return_value = mock_parser_instance
            mock_parser_class.return_value = mock_parser

            mock_router = Mock()
            mock_router_class.return_value = mock_router

            main()

        # Assert
        mock_router_class.assert_called_once_with(None, mock_logger, ANY)
        mock_router.route.assert_called_once_with("config", mock_args)

    def test_main_handles_config_clean_command_without_db_connection(self):
        """Test main() handles config --clean command without database connection."""
        # Arrange
        test_args = [CLI_COMMAND_PYTHON, "config", "--clean"]

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("sys.argv", test_args))
            mock_parser_class = stack.enter_context(patch("phasor_point_cli.cli.CLIArgumentParser"))
            mock_router_class = stack.enter_context(patch("phasor_point_cli.cli.CommandRouter"))
            mock_logging = stack.enter_context(patch("phasor_point_cli.cli.setup_logging"))

            mock_logger = Mock()
            mock_log_file = Mock()
            mock_logging.return_value = (mock_logger, mock_log_file)

            mock_parser = Mock()
            mock_parser_instance = Mock()
            mock_args = argparse.Namespace(command="config", verbose=False, clean=True)
            mock_parser_instance.parse_args.return_value = mock_args
            mock_parser.build.return_value = mock_parser_instance
            mock_parser_class.return_value = mock_parser

            mock_router = Mock()
            mock_router_class.return_value = mock_router

            main()

        # Assert
        mock_router_class.assert_called_once_with(None, mock_logger, ANY)
        mock_router.route.assert_called_once_with("config", mock_args)

    def test_main_routes_command_with_db_connection(self, monkeypatch):
        """Test main() routes commands that require database connection."""
        # Arrange
        monkeypatch.setenv("DB_HOST", "test_host")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_NAME", "test_db")
        monkeypatch.setenv("DB_USERNAME", "test_user")
        monkeypatch.setenv("DB_PASSWORD", "test_pass")

        test_args = [CLI_COMMAND_PYTHON, "list-tables"]

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("sys.argv", test_args))
            mock_parser_class = stack.enter_context(patch("phasor_point_cli.cli.CLIArgumentParser"))
            mock_cli_class = stack.enter_context(patch("phasor_point_cli.cli.PhasorPointCLI"))
            mock_router_class = stack.enter_context(patch("phasor_point_cli.cli.CommandRouter"))
            mock_logging = stack.enter_context(patch("phasor_point_cli.cli.setup_logging"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))

            mock_logger = Mock()
            mock_log_file = Mock()
            mock_logging.return_value = (mock_logger, mock_log_file)

            mock_parser = Mock()
            mock_parser_instance = Mock()
            mock_args = argparse.Namespace(
                command="list-tables",
                verbose=False,
                config=None,
                username="test_user",
                password="test_pass",
                connection_pool=3,
            )
            mock_parser_instance.parse_args.return_value = mock_args
            mock_parser.build.return_value = mock_parser_instance
            mock_parser_class.return_value = mock_parser

            mock_cli = Mock()
            mock_cli_class.return_value = mock_cli

            mock_router = Mock()
            mock_router_class.return_value = mock_router

            main()

        # Assert
        mock_cli_class.assert_called_once()
        mock_router_class.assert_called_once_with(mock_cli, mock_logger, ANY)
        mock_router.route.assert_called_once_with("list-tables", mock_args)
        mock_cli.cleanup_connections.assert_called_once()

    def test_main_cleans_up_connections_on_error(self, monkeypatch):
        """Test main() cleans up connections even when command fails."""
        # Arrange
        monkeypatch.setenv("DB_HOST", "test_host")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_NAME", "test_db")
        monkeypatch.setenv("DB_USERNAME", "test_user")
        monkeypatch.setenv("DB_PASSWORD", "test_pass")

        test_args = [CLI_COMMAND_PYTHON, "list-tables"]

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("sys.argv", test_args))
            mock_parser_class = stack.enter_context(patch("phasor_point_cli.cli.CLIArgumentParser"))
            mock_cli_class = stack.enter_context(patch("phasor_point_cli.cli.PhasorPointCLI"))
            mock_router_class = stack.enter_context(patch("phasor_point_cli.cli.CommandRouter"))
            mock_logging = stack.enter_context(patch("phasor_point_cli.cli.setup_logging"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))

            mock_logger = Mock()
            mock_log_file = Mock()
            mock_logging.return_value = (mock_logger, mock_log_file)

            mock_parser = Mock()
            mock_parser_instance = Mock()
            mock_args = argparse.Namespace(
                command="list-tables",
                verbose=False,
                config=None,
                username="test_user",
                password="test_pass",
                connection_pool=3,
            )
            mock_parser_instance.parse_args.return_value = mock_args
            mock_parser.build.return_value = mock_parser_instance
            mock_parser_class.return_value = mock_parser

            mock_cli = Mock()
            mock_cli_class.return_value = mock_cli

            mock_router = Mock()
            mock_router.route.side_effect = Exception("Command failed")
            mock_router_class.return_value = mock_router

            # Act & Assert
            with pytest.raises(Exception, match="Command failed"):
                main()

        # Assert - cleanup should still be called
        mock_cli.cleanup_connections.assert_called_once()

    def test_main_finds_config_file_automatically(self, monkeypatch):
        """Test main() finds config file using ConfigPathManager."""
        # Arrange
        monkeypatch.setenv("DB_HOST", "test_host")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_NAME", "test_db")
        monkeypatch.setenv("DB_USERNAME", "test_user")
        monkeypatch.setenv("DB_PASSWORD", "test_pass")

        test_args = [CLI_COMMAND_PYTHON, "list-tables"]

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("sys.argv", test_args))
            mock_parser_class = stack.enter_context(patch("phasor_point_cli.cli.CLIArgumentParser"))
            mock_cli_class = stack.enter_context(patch("phasor_point_cli.cli.PhasorPointCLI"))
            mock_router_class = stack.enter_context(patch("phasor_point_cli.cli.CommandRouter"))
            stack.enter_context(
                patch("phasor_point_cli.cli.setup_logging", return_value=(Mock(), Mock()))
            )
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            # Patch the import inside main's if block
            mock_path_mgr_class = stack.enter_context(
                patch("phasor_point_cli.config_paths.ConfigPathManager")
            )

            mock_parser = Mock()
            mock_parser_instance = Mock()
            mock_args = argparse.Namespace(
                command="list-tables",
                verbose=False,
                config=None,  # No config specified
                username="test_user",
                password="test_pass",
                connection_pool=3,
            )
            mock_parser_instance.parse_args.return_value = mock_args
            mock_parser.build.return_value = mock_parser_instance
            mock_parser_class.return_value = mock_parser

            mock_cli = Mock()
            mock_cli_class.return_value = mock_cli

            mock_router = Mock()
            mock_router_class.return_value = mock_router

            mock_path_mgr = Mock()
            mock_path_mgr.find_config_file.return_value = Path("/found/config.json")
            mock_path_mgr_class.return_value = mock_path_mgr

            main()

        # Assert
        mock_path_mgr.find_config_file.assert_called_once()
        mock_cli_class.assert_called_once()
        call_kwargs = mock_cli_class.call_args[1]
        # Compare as Path objects to handle platform differences (Windows backslashes vs Unix forward slashes)
        assert Path(call_kwargs["config_file"]) == Path("/found/config.json")

    def test_main_uses_verbose_flag(self):
        """Test main() uses verbose flag for logging."""
        # Arrange
        test_args = [CLI_COMMAND_PYTHON, "setup", "--verbose"]

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("sys.argv", test_args))
            mock_parser_class = stack.enter_context(patch("phasor_point_cli.cli.CLIArgumentParser"))
            stack.enter_context(patch("phasor_point_cli.cli.CommandRouter"))
            mock_logging = stack.enter_context(patch("phasor_point_cli.cli.setup_logging"))
            mock_logging.return_value = (Mock(), Mock())

            mock_parser = Mock()
            mock_parser_instance = Mock()
            mock_args = argparse.Namespace(command="setup", verbose=True)
            mock_parser_instance.parse_args.return_value = mock_args
            mock_parser.build.return_value = mock_parser_instance
            mock_parser_class.return_value = mock_parser

            main()

        # Assert
        mock_logging.assert_called_once_with(verbose=True)

    def test_main_uses_custom_connection_pool_size(self, monkeypatch):
        """Test main() uses custom connection pool size from args."""
        # Arrange
        monkeypatch.setenv("DB_HOST", "test_host")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_NAME", "test_db")
        monkeypatch.setenv("DB_USERNAME", "test_user")
        monkeypatch.setenv("DB_PASSWORD", "test_pass")

        test_args = [CLI_COMMAND_PYTHON, "extract", "--connection-pool", "7"]

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("sys.argv", test_args))
            mock_parser_class = stack.enter_context(patch("phasor_point_cli.cli.CLIArgumentParser"))
            mock_cli_class = stack.enter_context(patch("phasor_point_cli.cli.PhasorPointCLI"))
            stack.enter_context(patch("phasor_point_cli.cli.CommandRouter"))
            stack.enter_context(
                patch("phasor_point_cli.cli.setup_logging", return_value=(Mock(), Mock()))
            )
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))

            mock_parser = Mock()
            mock_parser_instance = Mock()
            mock_args = argparse.Namespace(
                command="extract",
                verbose=False,
                config=None,
                username="test_user",
                password="test_pass",
                connection_pool=7,  # Custom size
            )
            mock_parser_instance.parse_args.return_value = mock_args
            mock_parser.build.return_value = mock_parser_instance
            mock_parser_class.return_value = mock_parser

            mock_cli = Mock()
            mock_cli_class.return_value = mock_cli

            main()

        # Assert
        call_kwargs = mock_cli_class.call_args[1]
        assert call_kwargs["connection_pool_size"] == 7

    def test_main_command_router_uses_module_level_import(self, monkeypatch):
        """Test that main() uses module-level CommandRouter import (regression test for line 186 bug)."""
        # Arrange
        monkeypatch.setenv("DB_HOST", "test_host")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_NAME", "test_db")
        monkeypatch.setenv("DB_USERNAME", "test_user")
        monkeypatch.setenv("DB_PASSWORD", "test_pass")

        test_args = [CLI_COMMAND_PYTHON, "list-tables"]

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("sys.argv", test_args))
            mock_parser_class = stack.enter_context(patch("phasor_point_cli.cli.CLIArgumentParser"))
            mock_cli_class = stack.enter_context(patch("phasor_point_cli.cli.PhasorPointCLI"))
            # This patch at the module level ensures we're testing the import scope
            mock_router_class = stack.enter_context(patch("phasor_point_cli.cli.CommandRouter"))
            stack.enter_context(
                patch("phasor_point_cli.cli.setup_logging", return_value=(Mock(), Mock()))
            )
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))

            mock_parser = Mock()
            mock_parser_instance = Mock()
            mock_args = argparse.Namespace(
                command="list-tables",
                verbose=False,
                config=None,
                username="test_user",
                password="test_pass",
                connection_pool=3,
            )
            mock_parser_instance.parse_args.return_value = mock_args
            mock_parser.build.return_value = mock_parser_instance
            mock_parser_class.return_value = mock_parser

            mock_cli = Mock()
            mock_cli_class.return_value = mock_cli

            mock_router = Mock()
            mock_router_class.return_value = mock_router

            # This should not raise UnboundLocalError on line 186
            main()

        # Assert - CommandRouter was called with CLI instance (not None)
        assert mock_router_class.call_count == 1
        call_args = mock_router_class.call_args[0]
        assert call_args[0] == mock_cli  # First arg should be CLI instance


class TestEdgeCases:
    """Test suite for edge cases and error scenarios."""

    def test_phasorpoint_cli_with_invalid_port(self, monkeypatch):
        """Test PhasorPointCLI handles invalid port gracefully."""
        # Arrange
        monkeypatch.setenv("DB_HOST", "test_host")
        monkeypatch.setenv("DB_PORT", "invalid_port")  # Invalid
        monkeypatch.setenv("DB_NAME", "test_db")
        monkeypatch.setenv("DB_USERNAME", "test_user")
        monkeypatch.setenv("DB_PASSWORD", "test_pass")

        # Act & Assert - Now exits with clear error message instead of ValueError
        with ExitStack() as stack:
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))
            with pytest.raises(SystemExit) as exc_info:
                PhasorPointCLI(logger=Mock())
            assert exc_info.value.code == 1

    def test_main_with_explicit_config_file(self, monkeypatch):
        """Test main() with explicitly provided config file."""
        # Arrange
        monkeypatch.setenv("DB_HOST", "test_host")
        monkeypatch.setenv("DB_PORT", "5432")
        monkeypatch.setenv("DB_NAME", "test_db")
        monkeypatch.setenv("DB_USERNAME", "test_user")
        monkeypatch.setenv("DB_PASSWORD", "test_pass")

        test_args = [CLI_COMMAND_PYTHON, "--config", "my_config.json", "list-tables"]

        # Act
        with ExitStack() as stack:
            stack.enter_context(patch("sys.argv", test_args))
            mock_parser_class = stack.enter_context(patch("phasor_point_cli.cli.CLIArgumentParser"))
            mock_cli_class = stack.enter_context(patch("phasor_point_cli.cli.PhasorPointCLI"))
            stack.enter_context(patch("phasor_point_cli.cli.CommandRouter"))
            stack.enter_context(
                patch("phasor_point_cli.cli.setup_logging", return_value=(Mock(), Mock()))
            )
            stack.enter_context(patch("phasor_point_cli.cli.ConfigurationManager"))
            stack.enter_context(patch("phasor_point_cli.cli.ConfigPathManager"))

            # Setup parser mocks
            mock_parser = Mock()
            mock_parser_instance = Mock()
            mock_args = argparse.Namespace(
                command="list-tables",
                verbose=False,
                config="my_config.json",  # Explicit config
                username="test_user",
                password="test_pass",
                connection_pool=3,
            )
            mock_parser_instance.parse_args.return_value = mock_args
            mock_parser.build.return_value = mock_parser_instance
            mock_parser_class.return_value = mock_parser

            # Setup CLI mock
            mock_cli = Mock()
            mock_cli_class.return_value = mock_cli

            # Mock Path to make config file check pass
            mock_path_class = stack.enter_context(patch("phasor_point_cli.cli.Path"))
            mock_path_instance = Mock()
            mock_path_instance.exists.return_value = True
            mock_path_class.return_value = mock_path_instance

            main()

        # Assert
        call_kwargs = mock_cli_class.call_args[1]
        assert call_kwargs["config_file"] == "my_config.json"
