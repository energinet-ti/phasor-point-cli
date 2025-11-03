"""
Unit tests for CommandRouter class.

Tests the command routing logic that dispatches CLI commands to appropriate
handlers.
"""

import argparse
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from phasor_point_cli.command_router import CommandRouter
from phasor_point_cli.constants import CLI_COMMAND_PYTHON
from phasor_point_cli.models import ExtractionRequest, ExtractionResult, QueryResult


class TestCommandRouter:
    """Test suite for CommandRouter class."""

    @pytest.fixture
    def mock_cli(self):
        """Create a mock CLI instance."""
        from phasor_point_cli.models import PMUInfo

        cli = Mock()
        cli.connection_pool = Mock()
        cli.connection_pool.max_connections = 3

        # Create a mock config with get_pmu_info method
        mock_config = Mock()
        mock_config.get_pmu_info = Mock(return_value=PMUInfo(id=45012, station_name="Test PMU"))
        cli.config = mock_config
        cli.update_connection_pool_size = Mock()
        return cli

    @pytest.fixture
    def mock_logger(self):
        """Create a mock logger."""
        return Mock()

    @pytest.fixture
    def command_router(self, mock_cli, mock_logger):
        """Create a CommandRouter instance."""
        return CommandRouter(mock_cli, mock_logger, output=None)

    def test_initialization(self, mock_cli, mock_logger):
        """Test CommandRouter can be instantiated."""
        # Arrange & Act
        router = CommandRouter(mock_cli, mock_logger, output=None)

        # Assert
        assert router is not None
        assert router._cli == mock_cli
        assert router._logger == mock_logger

    def test_route_setup_command(self, command_router):
        """Test routing to setup command handler."""
        # Arrange
        args = argparse.Namespace(command="setup", force=False)

        # Act
        with patch.object(command_router, "handle_setup") as mock_handle:
            command_router.route("setup", args)

        # Assert
        mock_handle.assert_called_once_with(args)

    def test_route_list_tables_command(self, command_router):
        """Test routing to list-tables command handler."""
        # Arrange
        args = argparse.Namespace(command="list-tables", pmu=None, max_pmus=10)

        # Act
        with patch.object(command_router, "handle_list_tables") as mock_handle:
            command_router.route("list-tables", args)

        # Assert
        mock_handle.assert_called_once_with(args)

    def test_route_table_info_command(self, command_router):
        """Test routing to table-info command handler."""
        # Arrange
        args = argparse.Namespace(command="table-info", pmu=45012, resolution=1)

        # Act
        with patch.object(command_router, "handle_table_info") as mock_handle:
            command_router.route("table-info", args)

        # Assert
        mock_handle.assert_called_once_with(args)

    def test_route_extract_command(self, command_router):
        """Test routing to extract command handler."""
        # Arrange
        args = argparse.Namespace(command="extract", pmu=45012)

        # Act
        with patch.object(command_router, "handle_extract") as mock_handle:
            command_router.route("extract", args)

        # Assert
        mock_handle.assert_called_once_with(args)

    def test_route_batch_extract_command(self, command_router):
        """Test routing to batch-extract command handler."""
        # Arrange
        args = argparse.Namespace(command="batch-extract", pmus="45012,45013")

        # Act
        with patch.object(command_router, "handle_batch_extract") as mock_handle:
            command_router.route("batch-extract", args)

        # Assert
        mock_handle.assert_called_once_with(args)

    def test_route_query_command(self, command_router):
        """Test routing to query command handler."""
        # Arrange
        args = argparse.Namespace(command="query", sql="SELECT * FROM pmu_45012_1")

        # Act
        with patch.object(command_router, "handle_query") as mock_handle:
            command_router.route("query", args)

        # Assert
        mock_handle.assert_called_once_with(args)

    def test_route_unknown_command(self, command_router):
        """Test routing with unknown command raises ValueError."""
        # Arrange
        args = argparse.Namespace(command="unknown")

        # Act & Assert
        with pytest.raises(ValueError, match="Unknown command: unknown"):
            command_router.route("unknown", args)

    def test_handle_setup_without_force(self, command_router):
        """Test handle_setup without force flag (interactive by default)."""
        # Arrange
        args = argparse.Namespace(force=False, interactive=True)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.setup_configuration_files"
        ) as mock_setup:
            command_router.handle_setup(args)

        # Assert
        mock_setup.assert_called_once_with(
            force=False, local=False, interactive=True, refresh_pmus=False
        )

    def test_handle_setup_with_force(self, command_router):
        """Test handle_setup with force flag."""
        # Arrange
        args = argparse.Namespace(force=True, interactive=True)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.setup_configuration_files"
        ) as mock_setup:
            command_router.handle_setup(args)

        # Assert
        mock_setup.assert_called_once_with(
            force=True, local=False, interactive=True, refresh_pmus=False
        )

    def test_handle_setup_with_local(self, command_router):
        """Test handle_setup with local flag."""
        # Arrange
        args = argparse.Namespace(force=False, local=True, interactive=True)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.setup_configuration_files"
        ) as mock_setup:
            command_router.handle_setup(args)

        # Assert
        mock_setup.assert_called_once_with(
            force=False, local=True, interactive=True, refresh_pmus=False
        )

    def test_handle_setup_with_force_and_local(self, command_router):
        """Test handle_setup with both force and local flags."""
        # Arrange
        args = argparse.Namespace(force=True, local=True, interactive=True)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.setup_configuration_files"
        ) as mock_setup:
            command_router.handle_setup(args)

        # Assert
        mock_setup.assert_called_once_with(
            force=True, local=True, interactive=True, refresh_pmus=False
        )

    def test_handle_setup_with_interactive(self, command_router):
        """Test handle_setup with interactive flag (explicitly set)."""
        # Arrange
        args = argparse.Namespace(force=False, local=False, interactive=True)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.setup_configuration_files"
        ) as mock_setup:
            command_router.handle_setup(args)

        # Assert
        mock_setup.assert_called_once_with(
            force=False, local=False, interactive=True, refresh_pmus=False
        )

    def test_handle_setup_with_no_interactive(self, command_router):
        """Test handle_setup with --no-interactive flag."""
        # Arrange
        args = argparse.Namespace(force=False, local=False, interactive=False)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.setup_configuration_files"
        ) as mock_setup:
            command_router.handle_setup(args)

        # Assert
        mock_setup.assert_called_once_with(
            force=False, local=False, interactive=False, refresh_pmus=False
        )

    def test_handle_list_tables_default(self, command_router):
        """Test handle_list_tables with default parameters."""
        # Arrange
        args = argparse.Namespace(pmu=None, max_pmus=10, all=False)
        mock_result = Mock(found_pmus={45012: [1]}, total_tables=1)

        # Act
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.list_available_tables.return_value = mock_result
            command_router.handle_list_tables(args)

        # Assert
        mock_manager.list_available_tables.assert_called_once()
        call_kwargs = mock_manager.list_available_tables.call_args[1]
        assert call_kwargs["pmu_ids"] is None
        assert call_kwargs["max_pmus"] == 10

    def test_handle_list_tables_with_pmu_ids(self, command_router):
        """Test handle_list_tables with specific PMU numbers."""
        # Arrange
        args = argparse.Namespace(pmu=[45012, 45013], max_pmus=10, all=False)
        mock_result = Mock(found_pmus={45012: [1], 45013: [1]}, total_tables=2)

        # Act
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.list_available_tables.return_value = mock_result
            command_router.handle_list_tables(args)

        # Assert
        mock_manager.list_available_tables.assert_called_once()
        call_kwargs = mock_manager.list_available_tables.call_args[1]
        assert call_kwargs["pmu_ids"] == [45012, 45013]

    def test_handle_list_tables_with_all_flag(self, command_router):
        """Test handle_list_tables with --all flag."""
        # Arrange
        args = argparse.Namespace(pmu=None, max_pmus=10, all=True)
        mock_result = Mock(found_pmus={45012: [1]}, total_tables=1)

        # Act
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.list_available_tables.return_value = mock_result
            command_router.handle_list_tables(args)

        # Assert
        mock_manager.list_available_tables.assert_called_once()
        call_kwargs = mock_manager.list_available_tables.call_args[1]
        assert call_kwargs["max_pmus"] is None

    def test_handle_table_info(self, command_router):
        """Test handle_table_info."""
        # Arrange
        args = argparse.Namespace(pmu=45012, resolution=1)
        from phasor_point_cli.models import PMUInfo, TableInfo, TableStatistics

        mock_pmu_info = PMUInfo(id=45012, station_name="Test PMU")
        mock_stats = TableStatistics(row_count=1000, column_count=10)
        mock_table_info = TableInfo(
            pmu_id=45012,
            resolution=1,
            table_name="pmu_45012_1",
            statistics=mock_stats,
            pmu_info=mock_pmu_info,
            sample_data=None,
        )

        # Act
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.get_table_info.return_value = mock_table_info
            command_router.handle_table_info(args)

        # Assert
        mock_manager.get_table_info.assert_called_once_with(45012, 1)

    def test_handle_extract_with_minutes(self, command_router, mock_cli):
        """Test handle_extract with minutes duration."""
        # Arrange
        args = argparse.Namespace(
            pmu=45012,
            minutes=30,
            start=None,
            end=None,
            hours=None,
            days=None,
            resolution=1,
            output=None,
            processed=True,
            raw=False,
            no_clean=False,
            chunk_size=15,
            parallel=2,
            format="parquet",
            connection_pool=3,
        )

        # Create a mock request
        mock_request = Mock(spec=ExtractionRequest)

        mock_result = ExtractionResult(
            request=mock_request,
            success=True,
            output_file=Path("test.parquet"),
            rows_extracted=100,
            extraction_time_seconds=10.0,
        )

        # Act
        with patch("phasor_point_cli.command_router.ExtractionManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.extract.return_value = mock_result
            mock_manager_class.return_value = mock_manager

            command_router.handle_extract(args)

        # Assert
        mock_manager_class.assert_called_once()
        mock_manager.extract.assert_called_once()
        command_router._logger.info.assert_called_once()

    def test_handle_extract_with_error(self, command_router, mock_cli):
        """Test handle_extract with extraction error."""
        # Arrange
        args = argparse.Namespace(
            pmu=45012,
            minutes=30,
            start=None,
            end=None,
            hours=None,
            days=None,
            resolution=1,
            output=None,
            processed=True,
            raw=False,
            no_clean=False,
            chunk_size=15,
            parallel=2,
            format="parquet",
            connection_pool=3,
        )

        # Create a mock request
        mock_request = Mock(spec=ExtractionRequest)

        mock_result = ExtractionResult(
            request=mock_request,
            success=False,
            output_file=None,
            rows_extracted=0,
            extraction_time_seconds=0,
            error="Database connection failed",
        )

        # Act
        with patch("phasor_point_cli.command_router.ExtractionManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.extract.return_value = mock_result
            mock_manager_class.return_value = mock_manager

            command_router.handle_extract(args)

        # Assert
        command_router._logger.error.assert_called_once()
        assert "Database connection failed" in str(command_router._logger.error.call_args)

    def test_handle_extract_with_invalid_date_range(self, command_router):
        """Test handle_extract with invalid date range."""
        # Arrange
        args = argparse.Namespace(
            pmu=45012,
            minutes=None,
            start=None,
            end=None,
            hours=None,
            days=None,
            resolution=1,
            connection_pool=3,
        )

        # Act
        command_router.handle_extract(args)

        # Assert
        command_router._logger.error.assert_called_once()

    def test_handle_extract_with_raw_flag_disables_clean(self, command_router, mock_cli):
        """Test that --raw flag disables both processing and cleaning."""
        # Arrange
        args = argparse.Namespace(
            pmu=45012,
            minutes=30,
            start=None,
            end=None,
            hours=None,
            days=None,
            resolution=1,
            output=None,
            processed=True,
            raw=True,
            no_clean=False,
            chunk_size=15,
            parallel=2,
            format="parquet",
            connection_pool=3,
        )

        mock_result = ExtractionResult(
            request=Mock(spec=ExtractionRequest),
            success=True,
            output_file=Path("test.parquet"),
            rows_extracted=100,
            extraction_time_seconds=10.0,
        )

        # Act
        with patch("phasor_point_cli.command_router.ExtractionManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.extract.return_value = mock_result
            mock_manager_class.return_value = mock_manager

            command_router.handle_extract(args)

            # Get the ExtractionRequest that was created
            actual_request_call = mock_manager.extract.call_args[0][0]

        # Assert
        assert actual_request_call.processed is False, "--raw should disable processing"
        assert actual_request_call.clean is False, "--raw should disable cleaning"

    def test_handle_batch_extract(self, command_router):
        """Test handle_batch_extract."""
        # Arrange
        args = argparse.Namespace(
            pmus="45012,45013,45014",
            minutes=60,
            start=None,
            end=None,
            hours=None,
            days=None,
            output_dir="./output",
            resolution=1,
            processed=True,
            raw=False,
            no_clean=False,
            chunk_size=15,
            parallel=2,
            format="parquet",
            connection_pool=3,
        )
        from phasor_point_cli.models import BatchExtractionResult

        mock_batch_result = BatchExtractionResult(batch_id="test-batch-123", results=[])

        # Act
        with patch("phasor_point_cli.command_router.ExtractionManager") as MockEM:
            mock_manager = MockEM.return_value
            mock_manager.batch_extract.return_value = mock_batch_result
            command_router.handle_batch_extract(args)

        # Assert
        mock_manager.batch_extract.assert_called_once()
        call_args = mock_manager.batch_extract.call_args[0]
        requests = call_args[0]
        assert len(requests) == 3
        assert requests[0].pmu_id == 45012
        assert requests[1].pmu_id == 45013
        assert requests[2].pmu_id == 45014

    def test_handle_batch_extract_with_invalid_date_range(self, command_router):
        """Test handle_batch_extract with invalid date range."""
        # Arrange
        args = argparse.Namespace(
            pmus="45012,45013", minutes=None, start=None, end=None, hours=None, days=None
        )

        # Act
        command_router.handle_batch_extract(args)

        # Assert
        command_router._logger.error.assert_called_once()

    def test_handle_query_success(self, command_router):
        """Test handle_query with successful execution."""
        # Arrange
        args = argparse.Namespace(
            sql="SELECT * FROM pmu_45012_1", output="result.parquet", format="parquet"
        )

        mock_result = QueryResult(
            success=True,
            rows_returned=100,
            duration_seconds=2.5,
            output_file=Path("result.parquet"),
        )

        # Act
        with patch("phasor_point_cli.command_router.QueryExecutor") as mock_executor_class:
            mock_executor = Mock()
            mock_executor.execute.return_value = mock_result
            mock_executor_class.return_value = mock_executor

            command_router.handle_query(args)

        # Assert
        mock_executor.execute.assert_called_once_with(
            "SELECT * FROM pmu_45012_1", output_file="result.parquet", output_format="parquet"
        )
        command_router._logger.error.assert_not_called()

    def test_handle_query_failure(self, command_router):
        """Test handle_query with execution failure."""
        # Arrange
        args = argparse.Namespace(sql="SELECT * FROM invalid_table", output=None, format="parquet")

        mock_result = QueryResult(
            success=False,
            rows_returned=0,
            duration_seconds=0,
            output_file=None,
            error="Table not found",
        )

        # Act
        with patch("phasor_point_cli.command_router.QueryExecutor") as mock_executor_class:
            mock_executor = Mock()
            mock_executor.execute.return_value = mock_result
            mock_executor_class.return_value = mock_executor

            command_router.handle_query(args)

        # Assert
        command_router._logger.error.assert_called_once()
        assert "Table not found" in str(command_router._logger.error.call_args)

    def test_handle_extract_updates_connection_pool_size(self, command_router, mock_cli):
        """Test handle_extract updates connection pool size when needed."""
        # Arrange
        args = argparse.Namespace(
            pmu=45012,
            minutes=30,
            start=None,
            end=None,
            hours=None,
            days=None,
            resolution=1,
            output=None,
            processed=True,
            raw=False,
            no_clean=False,
            chunk_size=15,
            parallel=2,
            format="parquet",
            connection_pool=5,  # Different from default
        )

        # Create a mock request
        mock_request = Mock(spec=ExtractionRequest)

        mock_result = ExtractionResult(
            request=mock_request,
            success=True,
            output_file=Path("test.parquet"),
            rows_extracted=100,
            extraction_time_seconds=10.0,
        )

        # Act
        with patch("phasor_point_cli.command_router.ExtractionManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.extract.return_value = mock_result
            mock_manager_class.return_value = mock_manager

            command_router.handle_extract(args)

        # Assert
        mock_cli.update_connection_pool_size.assert_called_once_with(5)

    def test_handle_extract_with_pmu_not_in_config(self, command_router, mock_cli, capsys):
        """Test handle_extract with PMU not found in configuration."""
        # Arrange
        mock_cli.config.get_pmu_info = Mock(return_value=None)
        mock_cli.config.get_all_pmu_ids = Mock(return_value=[45020, 45021])

        args = argparse.Namespace(
            pmu=45012,
            minutes=30,
            start=None,
            end=None,
            hours=None,
            days=None,
            resolution=1,
            output=None,
            processed=True,
            raw=False,
            no_clean=False,
            chunk_size=15,
            parallel=2,
            format="parquet",
            connection_pool=3,
        )

        mock_result = ExtractionResult(
            request=Mock(spec=ExtractionRequest),
            success=True,
            output_file=Path("test.parquet"),
            rows_extracted=100,
            extraction_time_seconds=10.0,
        )

        # Act
        with patch("phasor_point_cli.command_router.ExtractionManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.extract.return_value = mock_result
            mock_manager_class.return_value = mock_manager

            command_router.handle_extract(args)

        # Assert
        captured = capsys.readouterr()
        assert "[WARNING] PMU 45012 not found in configuration" in captured.out
        assert "Configuration contains 2 other PMU(s)" in captured.out
        command_router._logger.warning.assert_called_once()

    def test_handle_extract_with_pmu_not_in_config_empty_config(
        self, command_router, mock_cli, capsys
    ):
        """Test handle_extract with PMU not found and empty configuration."""
        # Arrange
        mock_cli.config.get_pmu_info = Mock(return_value=None)
        mock_cli.config.get_all_pmu_ids = Mock(return_value=[])

        args = argparse.Namespace(
            pmu=45012,
            minutes=30,
            start=None,
            end=None,
            hours=None,
            days=None,
            resolution=1,
            output=None,
            processed=True,
            raw=False,
            no_clean=False,
            chunk_size=15,
            parallel=2,
            format="parquet",
            connection_pool=3,
        )

        mock_result = ExtractionResult(
            request=Mock(spec=ExtractionRequest),
            success=True,
            output_file=Path("test.parquet"),
            rows_extracted=100,
            extraction_time_seconds=10.0,
        )

        # Act
        with patch("phasor_point_cli.command_router.ExtractionManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.extract.return_value = mock_result
            mock_manager_class.return_value = mock_manager

            command_router.handle_extract(args)

        # Assert
        captured = capsys.readouterr()
        assert "[WARNING] PMU 45012 not found in configuration" in captured.out
        assert "No PMUs loaded in configuration (0 PMUs total)" in captured.out
        assert f"{CLI_COMMAND_PYTHON} setup --refresh-pmus" in captured.out

    def test_handle_setup_with_refresh_pmus(self, command_router):
        """Test handle_setup with --refresh-pmus flag."""
        # Arrange
        args = argparse.Namespace(force=False, local=False, interactive=True, refresh_pmus=True)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.setup_configuration_files"
        ) as mock_setup:
            command_router.handle_setup(args)

        # Assert
        mock_setup.assert_called_once_with(
            force=False, local=False, interactive=True, refresh_pmus=True
        )

    def test_handle_config_display(self, command_router, capsys):
        """Test handle_config displays configuration paths."""
        # Arrange
        args = argparse.Namespace(clean=False, local=False, all=False)

        mock_info = {
            "user_config_dir": "/home/user/.config/phasor-point-cli",
            "local_env": {"exists": True, "path": "/project/.env"},
            "local_config": {"exists": False, "path": "/project/config.json"},
            "user_env": {"exists": True, "path": "/home/user/.config/phasor-point-cli/.env"},
            "user_config": {
                "exists": True,
                "path": "/home/user/.config/phasor-point-cli/config.json",
            },
            "active_env": "/project/.env",
            "active_config": "/home/user/.config/phasor-point-cli/config.json",
        }

        # Act
        with patch("phasor_point_cli.config_paths.ConfigPathManager") as mock_path_mgr_class:
            mock_path_mgr = Mock()
            mock_path_mgr.get_config_locations_info.return_value = mock_info
            mock_path_mgr_class.return_value = mock_path_mgr

            command_router.handle_config(args)

        # Assert
        captured = capsys.readouterr()
        assert "PhasorPoint CLI Configuration Paths" in captured.out
        assert "[FOUND] /project/.env" in captured.out
        assert "[NOT FOUND] /project/config.json" in captured.out

    def test_handle_config_with_clean(self, command_router):
        """Test handle_config with --clean flag."""
        # Arrange
        args = argparse.Namespace(clean=True, local=False, all=False)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.cleanup_configuration_files"
        ) as mock_cleanup:
            command_router.handle_config(args)

        # Assert
        mock_cleanup.assert_called_once_with(local=False, all_locations=False)

    def test_handle_config_with_clean_local(self, command_router):
        """Test handle_config with --clean --local flags."""
        # Arrange
        args = argparse.Namespace(clean=True, local=True, all=False)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.cleanup_configuration_files"
        ) as mock_cleanup:
            command_router.handle_config(args)

        # Assert
        mock_cleanup.assert_called_once_with(local=True, all_locations=False)

    def test_handle_config_with_clean_all(self, command_router):
        """Test handle_config with --clean --all flags."""
        # Arrange
        args = argparse.Namespace(clean=True, local=False, all=True)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.cleanup_configuration_files"
        ) as mock_cleanup:
            command_router.handle_config(args)

        # Assert
        mock_cleanup.assert_called_once_with(local=False, all_locations=True)

    def test_handle_about(self, command_router):
        """Test handle_about command."""
        # Arrange
        args = argparse.Namespace()

        # Act
        with patch("phasor_point_cli.banner.print_about") as mock_print_about:
            command_router.handle_about(args)

        # Assert
        mock_print_about.assert_called_once()

    def test_route_config_command(self, command_router):
        """Test routing to config command handler."""
        # Arrange
        args = argparse.Namespace(command="config", clean=False)

        # Act
        with patch.object(command_router, "handle_config") as mock_handle:
            command_router.route("config", args)

        # Assert
        mock_handle.assert_called_once_with(args)

    def test_route_about_command(self, command_router):
        """Test routing to about command handler."""
        # Arrange
        args = argparse.Namespace(command="about")

        # Act
        with patch.object(command_router, "handle_about") as mock_handle:
            command_router.route("about", args)

        # Assert
        mock_handle.assert_called_once_with(args)

    def test_handle_list_tables_no_tables_found(self, command_router, mock_cli, capsys):
        """Test handle_list_tables when no tables are found."""
        # Arrange
        args = argparse.Namespace(pmu=None, max_pmus=10, all=False)
        mock_result = Mock(found_pmus={}, total_tables=0)
        mock_cli.config.get_all_pmu_ids = Mock(return_value=[45012, 45013])

        # Act
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.list_available_tables.return_value = mock_result
            command_router.handle_list_tables(args)

        # Assert
        command_router._logger.error.assert_called_once()
        captured = capsys.readouterr()
        assert "WARNING: No PMU Tables Found" in captured.out
        assert "Configuration contains 2 PMU(s)" in captured.out

    def test_handle_list_tables_no_tables_found_empty_config(
        self, command_router, mock_cli, capsys
    ):
        """Test handle_list_tables when no tables found and config is empty."""
        # Arrange
        args = argparse.Namespace(pmu=None, max_pmus=10, all=False)
        mock_result = Mock(found_pmus={}, total_tables=0)
        mock_cli.config.get_all_pmu_ids = Mock(return_value=[])

        # Act
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.list_available_tables.return_value = mock_result
            command_router.handle_list_tables(args)

        # Assert
        captured = capsys.readouterr()
        assert "WARNING: No PMU Tables Found" in captured.out
        assert "PMU metadata not loaded in configuration (0 PMUs in config)" in captured.out
        assert f"{CLI_COMMAND_PYTHON} setup --refresh-pmus" in captured.out

    def test_handle_list_tables_with_unknown_pmus(self, command_router, mock_cli, capsys):
        """Test handle_list_tables displays warning for unknown PMUs."""
        # Arrange
        args = argparse.Namespace(pmu=None, max_pmus=10, all=False)
        mock_result = Mock(found_pmus={45012: [1], 45999: [1]}, total_tables=2)
        mock_cli.config.get_pmu_info = Mock(
            side_effect=lambda pmu_id: None
            if pmu_id == 45999
            else Mock(station_name="Test PMU", country="US")
        )

        # Act
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.list_available_tables.return_value = mock_result
            command_router.handle_list_tables(args)

        # Assert
        captured = capsys.readouterr()
        assert "show as 'Unknown' - metadata not in configuration" in captured.out

    def test_handle_list_tables_exception_handling(self, command_router):
        """Test handle_list_tables stops progress tracker on exception."""
        # Arrange
        args = argparse.Namespace(pmu=None, max_pmus=10, all=False)

        # Act & Assert
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.list_available_tables.side_effect = Exception("Database error")

            with pytest.raises(Exception, match="Database error"):
                command_router.handle_list_tables(args)

    def test_handle_table_info_not_found(self, command_router, capsys):
        """Test handle_table_info when table is not found."""
        # Arrange
        args = argparse.Namespace(pmu=45012, resolution=1)

        # Act
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.get_table_info.return_value = None
            command_router.handle_table_info(args)

        # Assert
        command_router._logger.error.assert_called_once()
        captured = capsys.readouterr()
        assert "[ERROR] Table pmu_45012_1 not found or not accessible" in captured.out
        assert f"{CLI_COMMAND_PYTHON} setup --refresh-pmus" in captured.out

    def test_handle_table_info_with_country(self, command_router, capsys):
        """Test handle_table_info displays country information."""
        # Arrange
        args = argparse.Namespace(pmu=45012, resolution=1)
        from phasor_point_cli.models import PMUInfo, TableInfo, TableStatistics

        mock_pmu_info = PMUInfo(id=45012, station_name="Test PMU", country="USA")
        mock_stats = TableStatistics(row_count=1000, column_count=10)
        mock_table_info = TableInfo(
            pmu_id=45012,
            resolution=1,
            table_name="pmu_45012_1",
            statistics=mock_stats,
            pmu_info=mock_pmu_info,
            sample_data=None,
        )

        # Act
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.get_table_info.return_value = mock_table_info
            command_router.handle_table_info(args)

        # Assert
        captured = capsys.readouterr()
        assert "[PMU] 45012 - Test PMU (USA)" in captured.out

    def test_handle_batch_extract_with_missing_pmus(self, command_router, mock_cli, capsys):
        """Test handle_batch_extract with some PMUs not in config."""
        # Arrange
        mock_cli.config.get_pmu_info = Mock(
            side_effect=lambda pmu_id: None if pmu_id == 45013 else Mock(station_name="Test PMU")
        )
        mock_cli.config.get_all_pmu_ids = Mock(return_value=[45012, 45014])

        args = argparse.Namespace(
            pmus="45012,45013,45014",
            minutes=60,
            start=None,
            end=None,
            hours=None,
            days=None,
            output_dir="./output",
            resolution=1,
            processed=True,
            raw=False,
            no_clean=False,
            chunk_size=15,
            parallel=2,
            format="parquet",
            connection_pool=3,
        )
        from phasor_point_cli.models import BatchExtractionResult

        mock_batch_result = BatchExtractionResult(batch_id="test-batch-123", results=[])

        # Act
        with patch("phasor_point_cli.command_router.ExtractionManager") as MockEM:
            mock_manager = MockEM.return_value
            mock_manager.batch_extract.return_value = mock_batch_result
            command_router.handle_batch_extract(args)

        # Assert
        captured = capsys.readouterr()
        assert "[WARNING]" in captured.out
        assert "not found in configuration" in captured.out
        assert "45013" in captured.out

    def test_handle_batch_extract_with_missing_pmus_empty_config(
        self, command_router, mock_cli, capsys
    ):
        """Test handle_batch_extract with missing PMUs and empty configuration."""
        # Arrange
        mock_cli.config.get_pmu_info = Mock(return_value=None)
        mock_cli.config.get_all_pmu_ids = Mock(return_value=[])

        args = argparse.Namespace(
            pmus="45012,45013",
            minutes=60,
            start=None,
            end=None,
            hours=None,
            days=None,
        )

        # Act
        command_router.handle_batch_extract(args)

        # Assert
        captured = capsys.readouterr()
        assert "No PMUs loaded in configuration" in captured.out
        assert f"{CLI_COMMAND_PYTHON} setup --refresh-pmus" in captured.out

    def test_handle_batch_extract_updates_connection_pool(self, command_router, mock_cli):
        """Test handle_batch_extract updates connection pool size when needed."""
        # Arrange
        args = argparse.Namespace(
            pmus="45012,45013",
            minutes=60,
            start=None,
            end=None,
            hours=None,
            days=None,
            output_dir="./output",
            resolution=1,
            processed=True,
            raw=False,
            no_clean=False,
            chunk_size=15,
            parallel=2,
            format="parquet",
            connection_pool=8,  # Different from default
        )
        from phasor_point_cli.models import BatchExtractionResult

        mock_batch_result = BatchExtractionResult(batch_id="test-batch-123", results=[])

        # Act
        with patch("phasor_point_cli.command_router.ExtractionManager") as MockEM:
            mock_manager = MockEM.return_value
            mock_manager.batch_extract.return_value = mock_batch_result
            command_router.handle_batch_extract(args)

        # Assert
        mock_cli.update_connection_pool_size.assert_called_once_with(8)

    def test_handle_extract_with_verbose_timing(self, command_router, mock_cli):
        """Test handle_extract passes verbose_timing parameter."""
        # Arrange
        args = argparse.Namespace(
            pmu=45012,
            minutes=30,
            start=None,
            end=None,
            hours=None,
            days=None,
            resolution=1,
            output=None,
            processed=True,
            raw=False,
            no_clean=False,
            chunk_size=15,
            parallel=2,
            format="parquet",
            connection_pool=3,
            verbose_timing=True,
        )

        mock_result = ExtractionResult(
            request=Mock(spec=ExtractionRequest),
            success=True,
            output_file=Path("test.parquet"),
            rows_extracted=100,
            extraction_time_seconds=10.0,
        )

        # Act
        with patch("phasor_point_cli.command_router.ExtractionManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.extract.return_value = mock_result
            mock_manager_class.return_value = mock_manager

            command_router.handle_extract(args)

        # Assert
        call_kwargs = mock_manager_class.call_args[1]
        assert call_kwargs["verbose_timing"] is True

    def test_handle_batch_extract_with_verbose_timing(self, command_router):
        """Test handle_batch_extract passes verbose_timing parameter."""
        # Arrange
        args = argparse.Namespace(
            pmus="45012,45013",
            minutes=60,
            start=None,
            end=None,
            hours=None,
            days=None,
            output_dir="./output",
            resolution=1,
            processed=True,
            raw=False,
            no_clean=False,
            chunk_size=15,
            parallel=2,
            format="parquet",
            connection_pool=3,
            verbose_timing=True,
        )
        from phasor_point_cli.models import BatchExtractionResult

        mock_batch_result = BatchExtractionResult(batch_id="test-batch-123", results=[])

        # Act
        with patch("phasor_point_cli.command_router.ExtractionManager") as MockEM:
            mock_manager = MockEM.return_value
            mock_manager.batch_extract.return_value = mock_batch_result
            command_router.handle_batch_extract(args)

        # Assert
        call_kwargs = MockEM.call_args[1]
        assert call_kwargs["verbose_timing"] is True

    def test_handle_extract_with_replace_flag(self, command_router, mock_cli):
        """Test handle_extract passes replace parameter."""
        # Arrange
        args = argparse.Namespace(
            pmu=45012,
            minutes=30,
            start=None,
            end=None,
            hours=None,
            days=None,
            resolution=1,
            output=None,
            processed=True,
            raw=False,
            no_clean=False,
            chunk_size=15,
            parallel=2,
            format="parquet",
            connection_pool=3,
            replace=True,
        )

        mock_result = ExtractionResult(
            request=Mock(spec=ExtractionRequest),
            success=True,
            output_file=Path("test.parquet"),
            rows_extracted=100,
            extraction_time_seconds=10.0,
        )

        # Act
        with patch("phasor_point_cli.command_router.ExtractionManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager.extract.return_value = mock_result
            mock_manager_class.return_value = mock_manager

            command_router.handle_extract(args)

            # Get the ExtractionRequest that was created
            actual_request_call = mock_manager.extract.call_args[0][0]

        # Assert
        assert actual_request_call.replace is True

    def test_create_scan_progress_callback(self):
        """Test _create_scan_progress_callback returns working callback."""
        # Arrange
        from phasor_point_cli.command_router import _create_scan_progress_callback
        from phasor_point_cli.progress_tracker import ScanProgressTracker

        tracker = Mock(spec=ScanProgressTracker)
        callback = _create_scan_progress_callback(tracker)

        # Act
        callback(50, 100, 5)

        # Assert
        tracker.update.assert_called_once_with(50, 100, 5)

    def test_handle_table_info_with_time_range(self, command_router, capsys):
        """Test handle_table_info displays time range."""
        # Arrange
        args = argparse.Namespace(pmu=45012, resolution=1)
        from datetime import datetime

        from phasor_point_cli.models import PMUInfo, TableInfo, TableStatistics

        mock_pmu_info = PMUInfo(id=45012, station_name="Test PMU")
        mock_stats = TableStatistics(
            row_count=1000,
            column_count=10,
            start_time=datetime(2024, 1, 1, 0, 0, 0),
            end_time=datetime(2024, 1, 31, 23, 59, 59),
        )
        mock_table_info = TableInfo(
            pmu_id=45012,
            resolution=1,
            table_name="pmu_45012_1",
            statistics=mock_stats,
            pmu_info=mock_pmu_info,
            sample_data=None,
        )

        # Act
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.get_table_info.return_value = mock_table_info
            command_router.handle_table_info(args)

        # Assert
        captured = capsys.readouterr()
        assert "Time range:" in captured.out

    def test_handle_table_info_with_start_time_only(self, command_router, capsys):
        """Test handle_table_info displays only start time when end time is None."""
        # Arrange
        args = argparse.Namespace(pmu=45012, resolution=1)
        from datetime import datetime

        from phasor_point_cli.models import PMUInfo, TableInfo, TableStatistics

        mock_pmu_info = PMUInfo(id=45012, station_name="Test PMU")
        mock_stats = TableStatistics(
            row_count=1000,
            column_count=10,
            start_time=datetime(2024, 1, 1, 0, 0, 0),
            end_time=None,
        )
        mock_table_info = TableInfo(
            pmu_id=45012,
            resolution=1,
            table_name="pmu_45012_1",
            statistics=mock_stats,
            pmu_info=mock_pmu_info,
            sample_data=None,
        )

        # Act
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.get_table_info.return_value = mock_table_info
            command_router.handle_table_info(args)

        # Assert
        captured = capsys.readouterr()
        assert "Earliest timestamp:" in captured.out
        assert "Last day:" in captured.out

    def test_handle_table_info_with_sample_data(self, command_router, capsys):
        """Test handle_table_info displays sample data."""
        # Arrange
        args = argparse.Namespace(pmu=45012, resolution=1)
        from datetime import datetime

        import pandas as pd

        from phasor_point_cli.models import PMUInfo, TableInfo, TableStatistics

        mock_pmu_info = PMUInfo(id=45012, station_name="Test PMU")
        mock_stats = TableStatistics(
            row_count=1000,
            column_count=3,
            start_time=datetime(2024, 1, 1, 0, 0, 0),
            end_time=datetime(2024, 1, 31, 23, 59, 59),
        )
        sample_df = pd.DataFrame({"timestamp": [1, 2, 3], "voltage": [120.5, 121.0, 119.8]})
        mock_table_info = TableInfo(
            pmu_id=45012,
            resolution=1,
            table_name="pmu_45012_1",
            statistics=mock_stats,
            pmu_info=mock_pmu_info,
            sample_data=sample_df,
        )

        # Act
        with patch("phasor_point_cli.command_router.TableManager") as MockTableManager:
            mock_manager = MockTableManager.return_value
            mock_manager.get_table_info.return_value = mock_table_info
            command_router.handle_table_info(args)

        # Assert
        captured = capsys.readouterr()
        assert "SAMPLE DATA" in captured.out
        assert "first 5 rows" in captured.out

    def test_handle_config_display_with_env_vars(self, command_router, capsys, monkeypatch):
        """Test handle_config displays environment variables."""
        # Arrange
        args = argparse.Namespace(clean=False, local=False, all=False)
        monkeypatch.setenv("DB_HOST", "localhost")
        monkeypatch.setenv("DB_PASSWORD", "secret123")

        mock_info = {
            "user_config_dir": "/home/user/.config/phasor-point-cli",
            "local_env": {"exists": False, "path": "/project/.env"},
            "local_config": {"exists": False, "path": "/project/config.json"},
            "user_env": {"exists": False, "path": "/home/user/.config/phasor-point-cli/.env"},
            "user_config": {
                "exists": False,
                "path": "/home/user/.config/phasor-point-cli/config.json",
            },
            "active_env": None,
            "active_config": None,
        }

        # Act
        with patch("phasor_point_cli.config_paths.ConfigPathManager") as mock_path_mgr_class:
            mock_path_mgr = Mock()
            mock_path_mgr.get_config_locations_info.return_value = mock_info
            mock_path_mgr_class.return_value = mock_path_mgr

            command_router.handle_config(args)

        # Assert
        captured = capsys.readouterr()
        assert "DB_HOST=localhost" in captured.out
        assert "DB_PASSWORD=********" in captured.out
