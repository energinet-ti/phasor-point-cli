"""
Unit tests for CommandRouter class.

Tests the command routing logic that dispatches CLI commands to appropriate
handlers.
"""

import argparse
from unittest.mock import Mock, patch

import pytest

from phasor_point_cli.command_router import CommandRouter
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
        mock_config.get_pmu_info = Mock(
            return_value=PMUInfo(id=45012, station_name="Test PMU", region="Test Region")
        )
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
        return CommandRouter(mock_cli, mock_logger)

    def test_initialization(self, mock_cli, mock_logger):
        """Test CommandRouter can be instantiated."""
        # Arrange & Act
        router = CommandRouter(mock_cli, mock_logger)

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
        """Test handle_setup without force flag."""
        # Arrange
        args = argparse.Namespace(force=False)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.setup_configuration_files"
        ) as mock_setup:
            command_router.handle_setup(args)

        # Assert
        mock_setup.assert_called_once_with(
            force=False, local=False, interactive=False, refresh_pmus=False
        )

    def test_handle_setup_with_force(self, command_router):
        """Test handle_setup with force flag."""
        # Arrange
        args = argparse.Namespace(force=True)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.setup_configuration_files"
        ) as mock_setup:
            command_router.handle_setup(args)

        # Assert
        mock_setup.assert_called_once_with(
            force=True, local=False, interactive=False, refresh_pmus=False
        )

    def test_handle_setup_with_local(self, command_router):
        """Test handle_setup with local flag."""
        # Arrange
        args = argparse.Namespace(force=False, local=True)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.setup_configuration_files"
        ) as mock_setup:
            command_router.handle_setup(args)

        # Assert
        mock_setup.assert_called_once_with(
            force=False, local=True, interactive=False, refresh_pmus=False
        )

    def test_handle_setup_with_force_and_local(self, command_router):
        """Test handle_setup with both force and local flags."""
        # Arrange
        args = argparse.Namespace(force=True, local=True)

        # Act
        with patch(
            "phasor_point_cli.command_router.ConfigurationManager.setup_configuration_files"
        ) as mock_setup:
            command_router.handle_setup(args)

        # Assert
        mock_setup.assert_called_once_with(
            force=True, local=True, interactive=False, refresh_pmus=False
        )

    def test_handle_setup_with_interactive(self, command_router):
        """Test handle_setup with interactive flag."""
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

        mock_pmu_info = PMUInfo(id=45012, station_name="Test PMU", region="Test Region")
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
            output_file="test.parquet",
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
            success=True, rows_returned=100, duration_seconds=2.5, output_file="result.parquet"
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
            output_file="test.parquet",
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
