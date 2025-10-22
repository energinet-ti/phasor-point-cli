"""
File utility functions for PhasorPoint CLI.

Provides clean static methods for common file operations including filename
sanitization, directory management, and file size calculations.
"""

from __future__ import annotations

from pathlib import Path


class FileUtils:
    """Utility functions for file operations."""

    @staticmethod
    def sanitize_filename(name: str) -> str:
        """
        Sanitize a string to be safe for use in filenames.

        Replaces or removes characters that are problematic for filenames across
        different operating systems (Windows, macOS, Linux).

        Args:
            name: The name to sanitize

        Returns:
            Sanitized name safe for filenames

        Examples:
            >>> FileUtils.sanitize_filename("My File: Test")
            'My_File__Test'
            >>> FileUtils.sanitize_filename("")
            'unknown'
        """
        if not name:
            return "unknown"

        # Replace spaces with underscores
        sanitized = name.replace(" ", "_")

        # Remove or replace characters that are invalid in Windows filenames
        # Invalid chars: < > : " / \ | ? * and control characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            sanitized = sanitized.replace(char, "_")

        # Replace hash/pound sign which can be problematic
        sanitized = sanitized.replace("#", "_")

        # Remove any control characters (ASCII 0-31)
        sanitized = "".join(char for char in sanitized if ord(char) >= 32)

        # Remove leading/trailing dots and spaces
        sanitized = sanitized.strip(". ")

        # Collapse multiple underscores into single underscore
        while "__" in sanitized:
            sanitized = sanitized.replace("__", "_")

        # Ensure we don't end up with an empty string
        if not sanitized:
            sanitized = "unknown"

        return sanitized

    @staticmethod
    def ensure_directory_exists(path: str | Path) -> Path:
        """
        Ensure a directory exists, creating it if necessary.

        Creates parent directories as needed (equivalent to mkdir -p).

        Args:
            path: Directory path to ensure exists

        Returns:
            Path object representing the directory

        Raises:
            OSError: If directory cannot be created

        Examples:
            >>> FileUtils.ensure_directory_exists("data/exports")
            PosixPath('data/exports')
        """
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def get_file_size_mb(path: str | Path) -> float:
        """
        Get file size in megabytes.

        Args:
            path: Path to the file

        Returns:
            File size in megabytes (MB), rounded to 2 decimal places

        Raises:
            FileNotFoundError: If file doesn't exist
            OSError: If file cannot be accessed

        Examples:
            >>> FileUtils.get_file_size_mb("data.parquet")
            15.43
        """
        path = Path(path)
        size_bytes = path.stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        return round(size_mb, 2)

    @staticmethod
    def get_file_size_bytes(path: str | Path) -> int:
        """
        Get file size in bytes.

        Args:
            path: Path to the file

        Returns:
            File size in bytes

        Raises:
            FileNotFoundError: If file doesn't exist
            OSError: If file cannot be accessed
        """
        return Path(path).stat().st_size
