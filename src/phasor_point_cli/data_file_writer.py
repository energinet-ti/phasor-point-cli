"""
Data file writing utilities for PhasorPoint CLI.

Provides a unified interface for writing pandas DataFrames to various file
formats including Parquet and CSV.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .models import WriteResult


class DataFileWriter:
    """Writes DataFrames to various file formats."""

    def __init__(self, logger):
        """
        Initialize data file writer.

        Args:
            logger: Logger instance for output messages
        """
        self.logger = logger

    def write(
        self,
        df: pd.DataFrame,
        output_file: str | Path,
        format: str | None = None,
    ) -> WriteResult:
        """
        Write dataframe to file in specified format.

        If format is not specified, it will be inferred from the file extension.

        Args:
            df: DataFrame to write
            output_file: Path to output file
            format: Output format ('parquet' or 'csv'). If None, inferred from extension

        Returns:
            WriteResult with operation details

        Raises:
            ValueError: If format is unsupported
            IOError: If file cannot be written

        Examples:
            >>> writer = DataFileWriter(logger)
            >>> result = writer.write(df, "output.parquet")
            >>> print(result.success, result.file_size_mb)
            True 15.43
        """
        output_path = Path(output_file)

        # Infer format from extension if not provided
        if format is None:
            ext = output_path.suffix.lower().lstrip(".")
            if ext in ("parquet", "pq"):
                format = "parquet"
            elif ext in ("csv",):
                format = "csv"
            else:
                return WriteResult(
                    success=False,
                    output_file=output_path,
                    file_size_mb=0.0,
                    row_count=0,
                    column_count=0,
                    format="unknown",
                    error=f"Cannot infer format from extension '{ext}'. Please specify format explicitly.",
                )

        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if format == "parquet":
                self.write_parquet(df, output_path)
            elif format == "csv":
                self.write_csv(df, output_path)
            else:
                return WriteResult(
                    success=False,
                    output_file=output_path,
                    file_size_mb=0.0,
                    row_count=0,
                    column_count=0,
                    format=format,
                    error=f"Unsupported format '{format}'. Supported formats: parquet, csv",
                )

            # Get file size
            file_size_bytes = output_path.stat().st_size
            file_size_mb = round(file_size_bytes / (1024 * 1024), 2)

            self.logger.info("Saved %d rows to %s (%.2f MB)", len(df), output_path, file_size_mb)

            return WriteResult(
                success=True,
                output_file=output_path,
                file_size_mb=file_size_mb,
                row_count=len(df),
                column_count=len(df.columns),
                format=format,
                error=None,
            )

        except Exception as exc:
            self.logger.error("Failed to write file: %s", exc)
            return WriteResult(
                success=False,
                output_file=output_path,
                file_size_mb=0.0,
                row_count=0,
                column_count=0,
                format=format,
                error=str(exc),
            )

    def write_parquet(self, df: pd.DataFrame, output_file: str | Path) -> None:
        """
        Write dataframe to Parquet file.

        Args:
            df: DataFrame to write
            output_file: Path to output Parquet file

        Raises:
            IOError: If file cannot be written
        """
        output_path = Path(output_file)
        df.to_parquet(output_path, index=False)
        self.logger.debug("Wrote Parquet file: %s", output_path)

    def write_csv(self, df: pd.DataFrame, output_file: str | Path) -> None:
        """
        Write dataframe to CSV file.

        Args:
            df: DataFrame to write
            output_file: Path to output CSV file

        Raises:
            IOError: If file cannot be written
        """
        output_path = Path(output_file)
        df.to_csv(output_path, index=False, encoding="utf-8")
        self.logger.debug("Wrote CSV file: %s", output_path)
