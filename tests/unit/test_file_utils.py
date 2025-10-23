"""
Unit tests for FileUtils class.
"""

from pathlib import Path

import pytest

from phasor_point_cli.file_utils import FileUtils


class TestFileUtils:
    """Test suite for FileUtils class."""

    def test_sanitize_filename_basic(self):
        """Test basic filename sanitization."""
        assert FileUtils.sanitize_filename("My File") == "My_File"
        assert FileUtils.sanitize_filename("file_name") == "file_name"

    def test_sanitize_filename_invalid_chars(self):
        """Test sanitization of invalid characters."""
        assert (
            FileUtils.sanitize_filename("file<>name") == "file_name"
        )  # Multiple underscores collapsed
        assert FileUtils.sanitize_filename("file:name") == "file_name"
        assert FileUtils.sanitize_filename('file"name') == "file_name"
        assert FileUtils.sanitize_filename("file/name") == "file_name"
        assert FileUtils.sanitize_filename("file\\name") == "file_name"
        assert FileUtils.sanitize_filename("file|name") == "file_name"
        assert FileUtils.sanitize_filename("file?name") == "file_name"
        assert FileUtils.sanitize_filename("file*name") == "file_name"

    def test_sanitize_filename_hash(self):
        """Test sanitization of hash/pound signs."""
        assert FileUtils.sanitize_filename("file#name") == "file_name"

    def test_sanitize_filename_multiple_underscores(self):
        """Test collapsing of multiple underscores."""
        assert FileUtils.sanitize_filename("file___name") == "file_name"
        assert FileUtils.sanitize_filename("file  name") == "file_name"

    def test_sanitize_filename_empty(self):
        """Test sanitization of empty string."""
        assert FileUtils.sanitize_filename("") == "unknown"
        assert FileUtils.sanitize_filename(None) == "unknown"  # type: ignore[arg-type]

    def test_sanitize_filename_only_invalid(self):
        """Test sanitization when only invalid characters remain."""
        # After replacement and collapsing, we get single underscore
        assert FileUtils.sanitize_filename("<<<>>>") == "_"
        # Dots are stripped, leaving empty string -> "unknown"
        assert FileUtils.sanitize_filename("...") == "unknown"

    def test_ensure_directory_exists_creates_dir(self, tmp_path):
        """Test directory creation."""
        test_dir = tmp_path / "test" / "nested" / "dir"
        result = FileUtils.ensure_directory_exists(test_dir)

        assert result.exists()
        assert result.is_dir()
        assert result == test_dir

    def test_ensure_directory_exists_idempotent(self, tmp_path):
        """Test that creating existing directory doesn't fail."""
        test_dir = tmp_path / "existing"
        test_dir.mkdir()

        # Should not raise error
        result = FileUtils.ensure_directory_exists(test_dir)
        assert result.exists()
        assert result.is_dir()

    def test_ensure_directory_exists_with_string(self, tmp_path):
        """Test directory creation with string path."""
        test_dir = str(tmp_path / "string_path")
        result = FileUtils.ensure_directory_exists(test_dir)

        assert result.exists()
        assert result.is_dir()
        assert isinstance(result, Path)

    def test_get_file_size_mb(self, tmp_path):
        """Test file size calculation in MB."""
        test_file = tmp_path / "test.txt"

        # Create file with known size (1 MB = 1024 * 1024 bytes)
        content = "x" * (1024 * 1024)
        test_file.write_text(content)

        size_mb = FileUtils.get_file_size_mb(test_file)
        assert size_mb == 1.0

    def test_get_file_size_mb_small_file(self, tmp_path):
        """Test file size calculation for small files."""
        test_file = tmp_path / "small.txt"
        test_file.write_text("hello")

        size_mb = FileUtils.get_file_size_mb(test_file)
        assert size_mb == 0.0  # Rounded to 2 decimal places

    def test_get_file_size_mb_nonexistent(self, tmp_path):
        """Test file size for non-existent file."""
        test_file = tmp_path / "nonexistent.txt"

        with pytest.raises(FileNotFoundError):
            FileUtils.get_file_size_mb(test_file)

    def test_get_file_size_bytes(self, tmp_path):
        """Test file size calculation in bytes."""
        test_file = tmp_path / "test.txt"
        content = "hello world"
        test_file.write_text(content)

        size_bytes = FileUtils.get_file_size_bytes(test_file)
        assert size_bytes == len(content)

    def test_get_file_size_bytes_empty(self, tmp_path):
        """Test file size for empty file."""
        test_file = tmp_path / "empty.txt"
        test_file.touch()

        size_bytes = FileUtils.get_file_size_bytes(test_file)
        assert size_bytes == 0
