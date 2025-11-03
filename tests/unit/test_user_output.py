"""
Tests for the user output module.

Tests output formatters (Human and JSON) and the UserOutput orchestrator.
"""

import json
import warnings
from unittest.mock import patch

import pandas as pd
import pytest

from phasor_point_cli.user_output import HumanFormatter, JsonFormatter, OutputFormatter, UserOutput


class TestHumanFormatter:
    """Test HumanFormatter output formatting."""

    def test_section_header_format(self):
        """Test section header formatting with separator lines."""
        # Arrange
        formatter = HumanFormatter()

        # Act
        result = formatter.section_header("Test Section")

        # Assert
        assert "Test Section" in result
        assert "=" * 70 in result
        assert result.count("=" * 70) == 2

    def test_info_without_tag(self):
        """Test info message formatting without tag."""
        # Arrange
        formatter = HumanFormatter()

        # Act
        result = formatter.info("Test message")

        # Assert
        assert result == "Test message"

    def test_info_with_tag(self):
        """Test info message formatting with tag."""
        # Arrange
        formatter = HumanFormatter()

        # Act
        result = formatter.info("Test message", tag="INFO")

        # Assert
        assert result == "[INFO] Test message"

    def test_warning_format(self):
        """Test warning message formatting."""
        # Arrange
        formatter = HumanFormatter()

        # Act
        result = formatter.warning("Test warning")

        # Assert
        assert result == "[WARNING] Test warning"

    def test_data_summary_basic(self):
        """Test data summary with basic DataFrame."""
        # Arrange
        formatter = HumanFormatter()
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        # Act
        result = formatter.data_summary(df)

        # Assert
        assert "3 rows × 2 columns" in result
        assert "col1" in result
        assert "col2" in result
        assert "MB" in result

    def test_data_summary_with_title(self):
        """Test data summary with title."""
        # Arrange
        formatter = HumanFormatter()
        df = pd.DataFrame({"col1": [1, 2, 3]})

        # Act
        result = formatter.data_summary(df, title="Test Data")

        # Assert
        assert "Test Data" in result
        assert "=" * 70 in result

    def test_data_summary_with_timestamp(self):
        """Test data summary with timestamp column."""
        # Arrange
        formatter = HumanFormatter()
        df = pd.DataFrame(
            {
                "ts": pd.date_range("2024-01-01", periods=5, freq="h"),
                "value": [1, 2, 3, 4, 5],
            }
        )

        # Act
        result = formatter.data_summary(df)

        # Assert
        assert "Time range:" in result
        assert "2024-01-01" in result

    def test_data_summary_with_invalid_timestamp(self):
        """Test data summary with invalid timestamp column."""
        # Arrange
        formatter = HumanFormatter()
        df = pd.DataFrame({"ts": ["invalid", "timestamps", "here"], "value": [1, 2, 3]})

        # Act
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            result = formatter.data_summary(df)

        # Assert - should not crash and should not include time range
        assert "3 rows × 2 columns" in result
        # May or may not include time range depending on parsing

    def test_data_summary_many_columns(self):
        """Test data summary with many columns shows truncated list."""
        # Arrange
        formatter = HumanFormatter()
        columns = {f"col{i}": range(10) for i in range(35)}
        df = pd.DataFrame(columns)

        # Act
        result = formatter.data_summary(df)

        # Assert
        assert "35 total" in result
        assert "..." in result

    def test_data_summary_few_columns(self):
        """Test data summary with few columns shows all columns."""
        # Arrange
        formatter = HumanFormatter()
        df = pd.DataFrame({"col1": [1], "col2": [2], "col3": [3]})

        # Act
        result = formatter.data_summary(df)

        # Assert
        assert "col1" in result
        assert "col2" in result
        assert "col3" in result
        assert "..." not in result or "3 total" not in result

    def test_data_summary_dtype_counts(self):
        """Test data summary includes dtype information."""
        # Arrange
        formatter = HumanFormatter()
        df = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "str_col": ["a", "b", "c"],
                "float_col": [1.1, 2.2, 3.3],
            }
        )

        # Act
        result = formatter.data_summary(df)

        # Assert
        assert "Types:" in result

    def test_data_summary_memory_usage(self):
        """Test data summary includes memory usage."""
        # Arrange
        formatter = HumanFormatter()
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        # Act
        result = formatter.data_summary(df)

        # Assert
        assert "Memory:" in result
        assert "MB" in result

    def test_batch_progress_format(self):
        """Test batch progress message formatting."""
        # Arrange
        formatter = HumanFormatter()

        # Act
        result = formatter.batch_progress(5, 10, 123)

        # Assert
        assert "[BATCH]" in result
        assert "PMU 123" in result
        assert "(5/10)" in result

    def test_skip_message_format(self):
        """Test skip message formatting."""
        # Arrange
        formatter = HumanFormatter()

        # Act
        result = formatter.skip_message("/path/to/file.csv", "already exists")

        # Assert
        assert "[SKIP]" in result
        assert "/path/to/file.csv" in result
        assert "already exists" in result

    def test_batch_summary_all_successful(self):
        """Test batch summary with all successful."""
        # Arrange
        formatter = HumanFormatter()

        # Act
        result = formatter.batch_summary(10, 10, 0, 0, 45.5)

        # Assert
        assert "Total PMUs: 10" in result
        assert "Successful: 10" in result
        assert "Failed" not in result
        assert "Skipped" not in result
        assert "45.50s" in result
        assert "=" * 70 in result

    def test_batch_summary_with_failures(self):
        """Test batch summary with failures."""
        # Arrange
        formatter = HumanFormatter()

        # Act
        result = formatter.batch_summary(10, 7, 3, 0, 30.2)

        # Assert
        assert "Total PMUs: 10" in result
        assert "Successful: 7" in result
        assert "Failed: 3" in result

    def test_batch_summary_with_skipped(self):
        """Test batch summary with skipped items."""
        # Arrange
        formatter = HumanFormatter()

        # Act
        result = formatter.batch_summary(10, 8, 0, 2, 20.0)

        # Assert
        assert "Total PMUs: 10" in result
        assert "Successful: 8" in result
        assert "Skipped: 2" in result


class TestJsonFormatter:
    """Test JsonFormatter output formatting."""

    def test_section_header_json(self):
        """Test section header as JSON."""
        # Arrange
        formatter = JsonFormatter()

        # Act
        result = formatter.section_header("Test Section")
        data = json.loads(result)

        # Assert
        assert data["type"] == "section_header"
        assert data["title"] == "Test Section"

    def test_info_without_tag_json(self):
        """Test info message as JSON without tag."""
        # Arrange
        formatter = JsonFormatter()

        # Act
        result = formatter.info("Test message")
        data = json.loads(result)

        # Assert
        assert data["type"] == "info"
        assert data["message"] == "Test message"
        assert "tag" not in data

    def test_info_with_tag_json(self):
        """Test info message as JSON with tag."""
        # Arrange
        formatter = JsonFormatter()

        # Act
        result = formatter.info("Test message", tag="INFO")
        data = json.loads(result)

        # Assert
        assert data["type"] == "info"
        assert data["message"] == "Test message"
        assert data["tag"] == "INFO"

    def test_warning_json(self):
        """Test warning message as JSON."""
        # Arrange
        formatter = JsonFormatter()

        # Act
        result = formatter.warning("Test warning")
        data = json.loads(result)

        # Assert
        assert data["type"] == "warning"
        assert data["message"] == "Test warning"

    def test_data_summary_json_basic(self):
        """Test data summary as JSON."""
        # Arrange
        formatter = JsonFormatter()
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        # Act
        result = formatter.data_summary(df)
        data = json.loads(result)

        # Assert
        assert data["type"] == "data_summary"
        assert data["shape"] == [3, 2]
        assert "col1" in data["columns"]
        assert "col2" in data["columns"]
        assert "memory_bytes" in data
        assert isinstance(data["memory_bytes"], int)

    def test_data_summary_json_with_title(self):
        """Test data summary as JSON with title."""
        # Arrange
        formatter = JsonFormatter()
        df = pd.DataFrame({"col1": [1, 2, 3]})

        # Act
        result = formatter.data_summary(df, title="Test Data")
        data = json.loads(result)

        # Assert
        assert data["title"] == "Test Data"

    def test_data_summary_json_with_timestamp(self):
        """Test data summary as JSON with timestamp."""
        # Arrange
        formatter = JsonFormatter()
        df = pd.DataFrame(
            {
                "ts": pd.date_range("2024-01-01", periods=3, freq="h"),
                "value": [1, 2, 3],
            }
        )

        # Act
        result = formatter.data_summary(df)
        data = json.loads(result)

        # Assert
        assert "time_range" in data
        assert "start" in data["time_range"]
        assert "end" in data["time_range"]
        assert "2024-01-01" in data["time_range"]["start"]

    def test_data_summary_json_with_invalid_timestamp(self):
        """Test data summary as JSON with invalid timestamp."""
        # Arrange
        formatter = JsonFormatter()
        df = pd.DataFrame({"ts": ["invalid", "timestamps"], "value": [1, 2]})

        # Act
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            result = formatter.data_summary(df)
            data = json.loads(result)

        # Assert - should not crash
        assert data["type"] == "data_summary"
        # May or may not have time_range

    def test_data_summary_json_dtypes(self):
        """Test data summary JSON includes dtypes."""
        # Arrange
        formatter = JsonFormatter()
        df = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "str_col": ["a", "b", "c"],
            }
        )

        # Act
        result = formatter.data_summary(df)
        data = json.loads(result)

        # Assert
        assert "dtypes" in data
        assert "int_col" in data["dtypes"]
        assert "str_col" in data["dtypes"]

    def test_batch_progress_json(self):
        """Test batch progress as JSON."""
        # Arrange
        formatter = JsonFormatter()

        # Act
        result = formatter.batch_progress(5, 10, 123)
        data = json.loads(result)

        # Assert
        assert data["type"] == "batch_progress"
        assert data["completed"] == 5
        assert data["total"] == 10
        assert data["pmu_id"] == 123

    def test_skip_message_json(self):
        """Test skip message as JSON."""
        # Arrange
        formatter = JsonFormatter()

        # Act
        result = formatter.skip_message("/path/to/file.csv", "already exists")
        data = json.loads(result)

        # Assert
        assert data["type"] == "skip"
        assert data["filepath"] == "/path/to/file.csv"
        assert data["reason"] == "already exists"

    def test_batch_summary_json(self):
        """Test batch summary as JSON."""
        # Arrange
        formatter = JsonFormatter()

        # Act
        result = formatter.batch_summary(10, 8, 1, 1, 45.5)
        data = json.loads(result)

        # Assert
        assert data["type"] == "batch_summary"
        assert data["total"] == 10
        assert data["successful"] == 8
        assert data["failed"] == 1
        assert data["skipped"] == 1
        assert data["time_elapsed"] == 45.5

    def test_to_json_with_non_serializable(self):
        """Test JSON formatter handles non-serializable objects with default=str."""
        # Arrange
        formatter = JsonFormatter()

        # Act - _to_json uses default=str
        result = formatter._to_json({"date": pd.Timestamp("2024-01-01")})
        data = json.loads(result)

        # Assert - should not crash, converts to string
        assert "date" in data
        assert isinstance(data["date"], str)


class TestUserOutput:
    """Test UserOutput orchestrator."""

    def test_init_default_formatter(self):
        """Test UserOutput initializes with default HumanFormatter."""
        # Arrange & Act
        output = UserOutput()

        # Assert
        assert isinstance(output.formatter, HumanFormatter)
        assert output.quiet is False

    def test_init_custom_formatter(self):
        """Test UserOutput initializes with custom formatter."""
        # Arrange
        formatter = JsonFormatter()

        # Act
        output = UserOutput(formatter=formatter)

        # Assert
        assert output.formatter is formatter

    def test_init_quiet_mode(self):
        """Test UserOutput initializes in quiet mode."""
        # Arrange & Act
        output = UserOutput(quiet=True)

        # Assert
        assert output.quiet is True

    def test_is_tty_detection(self):
        """Test TTY detection."""
        # Arrange & Act
        with patch("sys.stdout.isatty", return_value=True):
            output = UserOutput()

        # Assert
        assert output.is_tty is True

    def test_section_header_prints(self):
        """Test section_header prints formatted output."""
        # Arrange
        output = UserOutput()

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.section_header("Test Section")
            mock_print.assert_called_once()
            args = mock_print.call_args[0][0]
            assert "Test Section" in args

    def test_section_header_quiet_mode(self):
        """Test section_header respects quiet mode."""
        # Arrange
        output = UserOutput(quiet=True)

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.section_header("Test Section")
            mock_print.assert_not_called()

    def test_info_prints(self):
        """Test info prints formatted message."""
        # Arrange
        output = UserOutput()

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.info("Test message")
            mock_print.assert_called_once()
            assert "Test message" in mock_print.call_args[0][0]

    def test_info_with_tag_prints(self):
        """Test info with tag prints formatted message."""
        # Arrange
        output = UserOutput()

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.info("Test message", tag="INFO")
            mock_print.assert_called_once()
            assert "[INFO]" in mock_print.call_args[0][0]

    def test_info_quiet_mode(self):
        """Test info respects quiet mode."""
        # Arrange
        output = UserOutput(quiet=True)

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.info("Test message")
            mock_print.assert_not_called()

    def test_warning_prints(self):
        """Test warning prints formatted message."""
        # Arrange
        output = UserOutput()

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.warning("Test warning")
            mock_print.assert_called_once()
            assert "[WARNING]" in mock_print.call_args[0][0]

    def test_warning_quiet_mode(self):
        """Test warning respects quiet mode."""
        # Arrange
        output = UserOutput(quiet=True)

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.warning("Test warning")
            mock_print.assert_not_called()

    def test_data_summary_prints(self):
        """Test data_summary prints formatted output."""
        # Arrange
        output = UserOutput()
        df = pd.DataFrame({"col1": [1, 2, 3]})

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.data_summary(df)
            mock_print.assert_called_once()
            args = mock_print.call_args[0][0]
            assert "3 rows" in args

    def test_data_summary_with_title_prints(self):
        """Test data_summary with title prints formatted output."""
        # Arrange
        output = UserOutput()
        df = pd.DataFrame({"col1": [1, 2, 3]})

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.data_summary(df, title="Test Data")
            mock_print.assert_called_once()
            args = mock_print.call_args[0][0]
            assert "Test Data" in args

    def test_data_summary_quiet_mode(self):
        """Test data_summary respects quiet mode."""
        # Arrange
        output = UserOutput(quiet=True)
        df = pd.DataFrame({"col1": [1, 2, 3]})

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.data_summary(df)
            mock_print.assert_not_called()

    def test_batch_progress_prints(self):
        """Test batch_progress prints formatted output."""
        # Arrange
        output = UserOutput()

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.batch_progress(5, 10, 123)
            mock_print.assert_called_once()
            args = mock_print.call_args[0][0]
            assert "PMU 123" in args

    def test_batch_progress_quiet_mode(self):
        """Test batch_progress respects quiet mode."""
        # Arrange
        output = UserOutput(quiet=True)

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.batch_progress(5, 10, 123)
            mock_print.assert_not_called()

    def test_skip_message_prints(self):
        """Test skip_message prints formatted output."""
        # Arrange
        output = UserOutput()

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.skip_message("/path/to/file.csv", "already exists")
            mock_print.assert_called_once()
            args = mock_print.call_args[0][0]
            assert "[SKIP]" in args

    def test_skip_message_quiet_mode(self):
        """Test skip_message respects quiet mode."""
        # Arrange
        output = UserOutput(quiet=True)

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.skip_message("/path/to/file.csv", "already exists")
            mock_print.assert_not_called()

    def test_batch_summary_prints(self):
        """Test batch_summary prints formatted output."""
        # Arrange
        output = UserOutput()

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.batch_summary(10, 8, 1, 1, 45.5)
            mock_print.assert_called_once()
            args = mock_print.call_args[0][0]
            assert "Total PMUs: 10" in args

    def test_batch_summary_quiet_mode(self):
        """Test batch_summary respects quiet mode."""
        # Arrange
        output = UserOutput(quiet=True)

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.batch_summary(10, 8, 1, 1, 45.5)
            mock_print.assert_not_called()

    def test_blank_line_prints(self):
        """Test blank_line prints empty line."""
        # Arrange
        output = UserOutput()

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.blank_line()
            mock_print.assert_called_once_with()

    def test_blank_line_quiet_mode(self):
        """Test blank_line respects quiet mode."""
        # Arrange
        output = UserOutput(quiet=True)

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.blank_line()
            mock_print.assert_not_called()


class TestUserOutputWithJsonFormatter:
    """Test UserOutput with JsonFormatter."""

    def test_json_output_all_methods(self):
        """Test all UserOutput methods work with JsonFormatter."""
        # Arrange
        formatter = JsonFormatter()
        output = UserOutput(formatter=formatter)
        df = pd.DataFrame({"col1": [1, 2, 3]})

        # Act & Assert - all should produce valid JSON
        with patch("builtins.print") as mock_print:
            output.section_header("Test")
            result = mock_print.call_args[0][0]
            data = json.loads(result)
            assert data["type"] == "section_header"

        with patch("builtins.print") as mock_print:
            output.info("Test", tag="INFO")
            result = mock_print.call_args[0][0]
            data = json.loads(result)
            assert data["type"] == "info"

        with patch("builtins.print") as mock_print:
            output.warning("Test")
            result = mock_print.call_args[0][0]
            data = json.loads(result)
            assert data["type"] == "warning"

        with patch("builtins.print") as mock_print:
            output.data_summary(df)
            result = mock_print.call_args[0][0]
            data = json.loads(result)
            assert data["type"] == "data_summary"

        with patch("builtins.print") as mock_print:
            output.batch_progress(1, 10, 123)
            result = mock_print.call_args[0][0]
            data = json.loads(result)
            assert data["type"] == "batch_progress"

        with patch("builtins.print") as mock_print:
            output.skip_message("file.csv", "reason")
            result = mock_print.call_args[0][0]
            data = json.loads(result)
            assert data["type"] == "skip"

        with patch("builtins.print") as mock_print:
            output.batch_summary(10, 8, 1, 1, 45.5)
            result = mock_print.call_args[0][0]
            data = json.loads(result)
            assert data["type"] == "batch_summary"


class TestOutputFormatterInterface:
    """Test OutputFormatter abstract base class."""

    def test_cannot_instantiate_abstract_class(self):
        """Test OutputFormatter cannot be instantiated directly."""
        # Act & Assert
        with pytest.raises(TypeError):
            OutputFormatter()  # type: ignore

    def test_custom_formatter_implementation(self):
        """Test custom formatter can be implemented."""

        # Arrange
        class CustomFormatter(OutputFormatter):
            def section_header(self, title: str) -> str:
                return f"CUSTOM: {title}"

            def info(self, message: str, tag: str | None = None) -> str:
                return f"CUSTOM INFO: {message}"

            def warning(self, message: str) -> str:
                return f"CUSTOM WARNING: {message}"

            def data_summary(self, df: pd.DataFrame, title: str | None = None) -> str:
                return f"CUSTOM SUMMARY: {len(df)} rows"

            def batch_progress(self, completed: int, total: int, pmu_id: int) -> str:
                return f"CUSTOM PROGRESS: {completed}/{total}"

            def skip_message(self, filepath: str, reason: str) -> str:
                return f"CUSTOM SKIP: {filepath}"

            def batch_summary(
                self, total: int, successful: int, failed: int, skipped: int, time_elapsed: float
            ) -> str:
                return f"CUSTOM SUMMARY: {successful}/{total}"

        formatter = CustomFormatter()
        output = UserOutput(formatter=formatter)

        # Act & Assert
        with patch("builtins.print") as mock_print:
            output.section_header("Test")
            assert "CUSTOM:" in mock_print.call_args[0][0]


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_empty_dataframe(self):
        """Test formatters handle empty DataFrame."""
        # Arrange
        human_formatter = HumanFormatter()
        json_formatter = JsonFormatter()
        df = pd.DataFrame()

        # Act & Assert - should not crash
        human_result = human_formatter.data_summary(df)
        assert "0 rows" in human_result

        json_result = json_formatter.data_summary(df)
        data = json.loads(json_result)
        assert data["shape"][0] == 0

    def test_large_dataframe(self):
        """Test formatters handle large DataFrame."""
        # Arrange
        human_formatter = HumanFormatter()
        json_formatter = JsonFormatter()
        df = pd.DataFrame({f"col{i}": range(1000) for i in range(50)})

        # Act & Assert - should not crash
        human_result = human_formatter.data_summary(df)
        assert "1,000 rows" in human_result

        json_result = json_formatter.data_summary(df)
        data = json.loads(json_result)
        assert data["shape"][0] == 1000

    def test_dataframe_with_nulls(self):
        """Test formatters handle DataFrame with null values."""
        # Arrange
        human_formatter = HumanFormatter()
        json_formatter = JsonFormatter()
        df = pd.DataFrame({"col1": [1, None, 3], "col2": [None, "b", None]})

        # Act & Assert - should not crash
        human_result = human_formatter.data_summary(df)
        assert "3 rows" in human_result

        json_result = json_formatter.data_summary(df)
        data = json.loads(json_result)
        assert data["shape"][0] == 3

    def test_special_characters_in_messages(self):
        """Test formatters handle special characters."""
        # Arrange
        human_formatter = HumanFormatter()
        json_formatter = JsonFormatter()
        message = "Test with special chars: \n\t\"quotes\" and 'apostrophes'"

        # Act & Assert - should not crash
        human_result = human_formatter.info(message)
        assert message in human_result

        json_result = json_formatter.info(message)
        data = json.loads(json_result)
        assert data["message"] == message

    def test_unicode_in_messages(self):
        """Test formatters handle Unicode characters."""
        # Arrange
        human_formatter = HumanFormatter()
        json_formatter = JsonFormatter()
        message = "Test Unicode: ñ, ü, 中文, emoji"

        # Act & Assert - should not crash
        human_result = human_formatter.info(message)
        assert "Unicode" in human_result

        json_result = json_formatter.info(message)
        data = json.loads(json_result)
        assert data["message"] == message

    def test_very_long_message(self):
        """Test formatters handle very long messages."""
        # Arrange
        human_formatter = HumanFormatter()
        json_formatter = JsonFormatter()
        message = "A" * 10000

        # Act & Assert - should not crash
        human_result = human_formatter.info(message)
        assert message in human_result

        json_result = json_formatter.info(message)
        data = json.loads(json_result)
        assert data["message"] == message

    def test_zero_values_in_batch_summary(self):
        """Test batch summary with all zeros."""
        # Arrange
        human_formatter = HumanFormatter()
        json_formatter = JsonFormatter()

        # Act
        human_result = human_formatter.batch_summary(0, 0, 0, 0, 0.0)
        json_result = json_formatter.batch_summary(0, 0, 0, 0, 0.0)

        # Assert
        assert "Total PMUs: 0" in human_result
        data = json.loads(json_result)
        assert data["total"] == 0

    def test_non_tty_environment(self):
        """Test UserOutput detects non-TTY environment."""
        # Arrange & Act
        with patch("sys.stdout.isatty", return_value=False):
            output = UserOutput()

        # Assert
        assert output.is_tty is False
