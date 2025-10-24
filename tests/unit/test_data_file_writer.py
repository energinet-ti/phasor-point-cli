"""
Unit tests for DataFileWriter class.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from phasor_point_cli.data_file_writer import DataFileWriter


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"], "col3": [1.5, 2.5, 3.5]})


@pytest.fixture
def writer():
    """Create DataFileWriter instance with mock logger."""
    logger = MagicMock()
    return DataFileWriter(logger)


class TestDataFileWriter:
    """Test suite for DataFileWriter class."""

    def test_write_parquet_success(self, writer, sample_df, tmp_path):
        """Test successful Parquet file writing."""
        # Arrange
        output_file = tmp_path / "test.parquet"

        # Act
        result = writer.write(sample_df, output_file, format="parquet")

        # Assert
        assert result.success is True
        assert result.output_file == output_file
        assert result.row_count == 3
        assert result.column_count == 3
        assert result.format == "parquet"
        assert result.file_size_mb >= 0
        assert result.error is None
        assert output_file.exists()

    def test_write_csv_success(self, writer, sample_df, tmp_path):
        """Test successful CSV file writing."""
        # Arrange
        output_file = tmp_path / "test.csv"

        # Act
        result = writer.write(sample_df, output_file, format="csv")

        # Assert
        assert result.success is True
        assert result.output_file == output_file
        assert result.row_count == 3
        assert result.column_count == 3
        assert result.format == "csv"
        assert result.file_size_mb >= 0
        assert result.error is None
        assert output_file.exists()

    def test_write_format_inference_parquet(self, writer, sample_df, tmp_path):
        """Test format inference from .parquet extension."""
        # Arrange
        output_file = tmp_path / "test.parquet"

        # Act
        result = writer.write(sample_df, output_file)

        # Assert
        assert result.success is True
        assert result.format == "parquet"
        assert output_file.exists()

    def test_write_format_inference_csv(self, writer, sample_df, tmp_path):
        """Test format inference from .csv extension."""
        # Arrange
        output_file = tmp_path / "test.csv"

        # Act
        result = writer.write(sample_df, output_file)

        # Assert
        assert result.success is True
        assert result.format == "csv"
        assert output_file.exists()

    def test_write_format_inference_pq(self, writer, sample_df, tmp_path):
        """Test format inference from .pq extension."""
        # Arrange
        output_file = tmp_path / "test.pq"

        # Act
        result = writer.write(sample_df, output_file)

        # Assert
        assert result.success is True
        assert result.format == "parquet"
        assert output_file.exists()

    def test_write_unsupported_format(self, writer, sample_df, tmp_path):
        """Test handling of unsupported format."""
        # Arrange
        output_file = tmp_path / "test.xlsx"

        # Act
        result = writer.write(sample_df, output_file, format="xlsx")

        # Assert
        assert result.success is False
        assert "Unsupported format" in result.error
        assert result.row_count == 0
        assert result.column_count == 0

    def test_write_unknown_extension(self, writer, sample_df, tmp_path):
        """Test handling of unknown file extension."""
        # Arrange
        output_file = tmp_path / "test.unknown"

        # Act
        result = writer.write(sample_df, output_file)

        # Assert
        assert result.success is False
        assert "Cannot infer format" in result.error

    def test_write_creates_parent_directory(self, writer, sample_df, tmp_path):
        """Test that parent directories are created if needed."""
        # Arrange
        output_file = tmp_path / "nested" / "path" / "test.csv"

        # Act
        result = writer.write(sample_df, output_file, format="csv")

        # Assert
        assert result.success is True
        assert output_file.exists()
        assert output_file.parent.exists()

    def test_write_parquet_direct(self, writer, sample_df, tmp_path):
        """Test direct Parquet writing method."""
        # Arrange
        output_file = tmp_path / "test.parquet"

        # Act
        writer.write_parquet(sample_df, output_file)

        # Assert
        assert output_file.exists()
        df_read = pd.read_parquet(output_file)
        pd.testing.assert_frame_equal(df_read, sample_df)

    def test_write_csv_direct(self, writer, sample_df, tmp_path):
        """Test direct CSV writing method."""
        # Arrange
        output_file = tmp_path / "test.csv"

        # Act
        writer.write_csv(sample_df, output_file)

        # Assert
        assert output_file.exists()
        df_read = pd.read_csv(output_file)
        pd.testing.assert_frame_equal(df_read, sample_df)

    def test_write_empty_dataframe(self, writer, tmp_path):
        """Test writing empty DataFrame."""
        # Arrange
        empty_df = pd.DataFrame()
        output_file = tmp_path / "empty.csv"

        # Act
        result = writer.write(empty_df, output_file, format="csv")

        # Assert
        assert result.success is True
        assert result.row_count == 0
        assert output_file.exists()

    def test_write_large_dataframe(self, writer, tmp_path):
        """Test writing larger DataFrame."""
        # Arrange
        large_df = pd.DataFrame({f"col{i}": range(1000) for i in range(10)})
        output_file = tmp_path / "large.parquet"

        # Act
        result = writer.write(large_df, output_file, format="parquet")

        # Assert
        assert result.success is True
        assert result.row_count == 1000
        assert result.column_count == 10
        assert result.file_size_mb > 0
