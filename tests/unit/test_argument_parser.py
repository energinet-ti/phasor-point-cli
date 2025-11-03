"""
Unit tests for CLIArgumentParser class.

Tests the argument parser builder class that creates and configures the
CLI argument parser with all commands and options.
"""

import argparse

from phasor_point_cli.argument_parser import CLIArgumentParser
from phasor_point_cli.constants import CLI_COMMAND_PYTHON


class TestCLIArgumentParser:
    """Test suite for CLIArgumentParser class."""

    def test_initialization(self):
        """Test CLIArgumentParser can be instantiated."""
        # Arrange & Act
        parser_builder = CLIArgumentParser()

        # Assert
        assert parser_builder is not None
        assert isinstance(parser_builder, CLIArgumentParser)

    def test_build_returns_argument_parser(self):
        """Test build() returns an ArgumentParser instance."""
        # Arrange
        parser_builder = CLIArgumentParser()

        # Act
        parser = parser_builder.build()

        # Assert
        assert isinstance(parser, argparse.ArgumentParser)
        # Check that description contains banner and key text
        assert parser.description is not None
        assert "PMU Data Extraction Tool" in parser.description
        assert "COMMAND GROUPS" in parser.description
        assert "Configuration:" in parser.description
        assert "Data Extraction:" in parser.description
        assert parser.prog == CLI_COMMAND_PYTHON

    def test_global_arguments_present(self):
        """Test global arguments are configured."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(
            ["--config", "test.json", "--username", "user", "--password", "pass", "setup"]
        )

        # Assert
        assert args.config == "test.json"
        assert args.username == "user"
        assert args.password == "pass"

    def test_setup_command_configuration(self):
        """Test setup command is properly configured."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["setup"])

        # Assert
        assert args.command == "setup"
        assert hasattr(args, "force")
        assert args.force is False

    def test_setup_command_with_force_flag(self):
        """Test setup command with --force flag."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["setup", "--force"])

        # Assert
        assert args.command == "setup"
        assert args.force is True

    def test_list_tables_command_configuration(self):
        """Test list-tables command is properly configured."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["list-tables"])

        # Assert
        assert args.command == "list-tables"
        assert hasattr(args, "pmu")
        assert hasattr(args, "max_pmus")
        assert args.max_pmus == 10  # default value

    def test_list_tables_command_with_pmu_numbers(self):
        """Test list-tables command with specific PMU numbers."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["list-tables", "--pmu", "45012", "45013"])

        # Assert
        assert args.command == "list-tables"
        assert args.pmu == [45012, 45013]

    def test_list_tables_command_with_all_flag(self):
        """Test list-tables command with --all flag."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["list-tables", "--all"])

        # Assert
        assert args.command == "list-tables"
        assert args.all is True

    def test_table_info_command_configuration(self):
        """Test table-info command is properly configured."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["table-info", "--pmu", "45012"])

        # Assert
        assert args.command == "table-info"
        assert args.pmu == 45012
        assert args.resolution == 50  # default value

    def test_table_info_command_with_resolution(self):
        """Test table-info command with custom resolution."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["table-info", "--pmu", "45012", "--resolution", "10"])

        # Assert
        assert args.command == "table-info"
        assert args.pmu == 45012
        assert args.resolution == 10

    def test_extract_command_configuration(self):
        """Test extract command is properly configured."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["extract", "--pmu", "45012", "--minutes", "30"])

        # Assert
        assert args.command == "extract"
        assert args.pmu == 45012
        assert args.minutes == 30
        assert args.resolution == 50
        assert args.format == "csv"
        assert args.chunk_size == 15
        assert args.parallel == 2
        assert args.connection_pool == 3

    def test_extract_command_with_start_end_dates(self):
        """Test extract command with start and end dates."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(
            [
                "extract",
                "--pmu",
                "45012",
                "--start",
                "2025-01-01 00:00:00",
                "--end",
                "2025-01-01 01:00:00",
            ]
        )

        # Assert
        assert args.command == "extract"
        assert args.start == "2025-01-01 00:00:00"
        assert args.end == "2025-01-01 01:00:00"

    def test_extract_command_with_raw_flag(self):
        """Test extract command with --raw flag."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["extract", "--pmu", "45012", "--minutes", "30", "--raw"])

        # Assert
        assert args.command == "extract"
        assert args.raw is True

    def test_extract_command_with_output_format(self):
        """Test extract command with CSV output format."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(
            ["extract", "--pmu", "45012", "--minutes", "30", "--format", "csv"]
        )

        # Assert
        assert args.command == "extract"
        assert args.format == "csv"

    def test_extract_command_with_custom_chunk_settings(self):
        """Test extract command with custom chunk size and parallelism."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(
            [
                "extract",
                "--pmu",
                "45012",
                "--minutes",
                "120",
                "--chunk-size",
                "30",
                "--parallel",
                "4",
                "--connection-pool",
                "5",
            ]
        )

        # Assert
        assert args.chunk_size == 30
        assert args.parallel == 4
        assert args.connection_pool == 5

    def test_batch_extract_command_configuration(self):
        """Test batch-extract command is properly configured."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(
            ["batch-extract", "--pmus", "45012,45013,45014", "--minutes", "60"]
        )

        # Assert
        assert args.command == "batch-extract"
        assert args.pmus == "45012,45013,45014"
        assert args.minutes == 60

    def test_batch_extract_command_with_output_dir(self):
        """Test batch-extract command with output directory."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(
            [
                "batch-extract",
                "--pmus",
                "45012,45013",
                "--minutes",
                "30",
                "--output-dir",
                "./output",
            ]
        )

        # Assert
        assert args.command == "batch-extract"
        assert args.output_dir == "./output"

    def test_query_command_configuration(self):
        """Test query command is properly configured."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["query", "--sql", "SELECT * FROM pmu_45012_1"])

        # Assert
        assert args.command == "query"
        assert args.sql == "SELECT * FROM pmu_45012_1"
        assert args.format == "parquet"  # default

    def test_query_command_with_output_file(self):
        """Test query command with output file."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(
            [
                "query",
                "--sql",
                "SELECT * FROM pmu_45012_1",
                "--output",
                "result.parquet",
                "--format",
                "csv",
            ]
        )

        # Assert
        assert args.output == "result.parquet"
        assert args.format == "csv"

    def test_no_command_specified(self):
        """Test parser when no command is specified."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args([])

        # Assert
        assert args.command is None

    def test_extract_command_verbose_flag(self):
        """Test extract command with verbose flag."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["extract", "--pmu", "45012", "--minutes", "30", "--verbose"])

        # Assert
        assert args.verbose is True

    def test_extract_command_diagnostics_flag(self):
        """Test extract command with diagnostics flag."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["extract", "--pmu", "45012", "--minutes", "30", "--diagnostics"])

        # Assert
        assert args.diagnostics is True

    def test_extract_command_no_clean_flag(self):
        """Test extract command with no-clean flag."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["extract", "--pmu", "45012", "--minutes", "30", "--no-clean"])

        # Assert
        assert args.no_clean is True

    def test_aboot_command_hidden_easter_egg(self):
        """Test hidden aboot command (easter egg) is properly configured."""
        # Arrange
        parser_builder = CLIArgumentParser()
        parser = parser_builder.build()

        # Act
        args = parser.parse_args(["aboot"])

        # Assert
        assert args.command == "aboot"
