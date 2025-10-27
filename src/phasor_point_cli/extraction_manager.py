"""
High-level extraction manager that coordinates data retrieval, processing, and persistence.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

from .chunk_strategy import ChunkStrategy
from .data_extractor import DataExtractor
from .data_processor import DataProcessor
from .data_validator import DataValidator
from .file_utils import FileUtils
from .models import BatchExtractionResult, ExtractionRequest, ExtractionResult
from .power_calculator import PowerCalculator


class ExtractionManager:
    """Facade that orchestrates extraction, processing, power calculations, and persistence."""

    def __init__(
        self,
        connection_pool,
        config_manager,
        logger,
        *,
        data_extractor: DataExtractor | None = None,
        data_processor: DataProcessor | None = None,
        power_calculator: PowerCalculator | None = None,
    ) -> None:
        self.connection_pool = connection_pool
        self.config_manager = config_manager
        self.logger = logger

        validator = None
        if data_processor is None:
            validator = DataValidator(logger=logger)

        self.data_processor = data_processor or DataProcessor(
            config_manager=config_manager, logger=logger, validator=validator
        )
        self.data_extractor = data_extractor or DataExtractor(
            connection_pool=connection_pool, logger=logger
        )
        self.power_calculator = power_calculator or PowerCalculator(logger=logger)

    # ------------------------------------------------------------------ Helpers
    def _config(self):
        if hasattr(self.config_manager, "config"):
            return self.config_manager.config
        return self.config_manager or {}

    @staticmethod
    def _get_local_timezone():
        """Get local timezone, preferring TZ environment variable."""
        tz_env = os.environ.get("TZ")
        if tz_env:
            try:
                return pytz.timezone(tz_env)
            except Exception:
                pass
        return datetime.now().astimezone().tzinfo

    @staticmethod
    def _get_utc_offset(dt: datetime, local_tz) -> str:
        """
        Get UTC offset string for a specific datetime in the given timezone.

        Args:
            dt: Naive datetime to check offset for
            local_tz: Timezone to use for offset calculation

        Returns:
            Offset string in format "+HH:MM" or "-HH:MM"
        """
        try:
            if local_tz is None:
                return "+00:00"

            # Localize the naive datetime to get timezone-aware version
            if hasattr(local_tz, "localize"):
                # pytz timezone
                aware_dt = local_tz.localize(dt, is_dst=True)
            else:
                # other timezone implementations
                aware_dt = dt.replace(tzinfo=local_tz)

            # Get offset in seconds
            offset_seconds = aware_dt.utcoffset().total_seconds() if aware_dt.utcoffset() else 0
            offset_hours = int(offset_seconds // 3600)
            offset_minutes = int((abs(offset_seconds) % 3600) // 60)

            sign = "+" if offset_seconds >= 0 else "-"
            return f"{sign}{abs(offset_hours):02d}:{offset_minutes:02d}"
        except Exception:
            return "+00:00"

    def _get_station_name(self, pmu_id: int) -> str:
        """Get sanitized station name from PMU ID."""
        pmu_info = self.config_manager.get_pmu_info(pmu_id)
        station_name = pmu_info.station_name if pmu_info else "unknown"
        return FileUtils.sanitize_filename(station_name)

    def _build_default_output_path(self, request: ExtractionRequest) -> Path:
        station_name = self._get_station_name(request.pmu_id)
        start_str = request.date_range.start.strftime("%Y%m%d_%H%M%S")
        end_str = request.date_range.end.strftime("%Y%m%d_%H%M%S")
        filename = f"pmu_{request.pmu_id}_{station_name}_{request.resolution}hz_{start_str}_to_{end_str}.{request.output_format}"
        return Path(filename)

    def _resolve_output_path(self, request: ExtractionRequest) -> Path:
        return (
            Path(request.output_file)
            if request.output_file
            else self._build_default_output_path(request)
        )

    def _write_output(self, df: pd.DataFrame, output_path: Path, output_format: str) -> float:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_format == "csv":
            df.to_csv(output_path, index=False, encoding="utf-8")
        elif output_format == "parquet":
            df.to_parquet(output_path, index=False)
        else:
            raise ValueError(f"Unsupported output format '{output_format}'")
        return output_path.stat().st_size / 1024 / 1024

    def _write_extraction_log(self, log_data: dict, output_path: Path) -> None:
        log_file = output_path.with_name(output_path.stem + "_extraction_log.json")
        with log_file.open("w", encoding="utf-8") as handle:
            json.dump(log_data, handle, indent=2, ensure_ascii=False)

    def _read_extraction_log(self, output_path: Path) -> dict | None:
        """Read extraction log if it exists."""
        log_file = output_path.with_name(output_path.stem + "_extraction_log.json")
        if not log_file.exists():
            return None
        try:
            with log_file.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except Exception as exc:
            self.logger.warning(f"Could not read extraction log: {exc}")
            return None

    def _check_existing_file(
        self, request: ExtractionRequest, output_path: Path
    ) -> tuple[bool, str]:
        """
        Check if output file exists and matches the requested parameters.

        Returns:
            (should_skip, reason) - tuple indicating if extraction should be skipped and why
        """
        # If replace flag is set, never skip
        if request.replace:
            if output_path.exists():
                self.logger.info(f"Replacing existing file: {output_path}")
            return False, "replace flag set" if request.replace else "file does not exist"

        # If skip_existing is False, never skip
        if not request.skip_existing:
            return False, "skip_existing flag is False"

        # Check if file exists
        if not output_path.exists():
            return False, "file does not exist"

        # Read extraction log to compare parameters
        extraction_log = self._read_extraction_log(output_path)
        if not extraction_log:
            # No log file, can't verify if data matches - allow user to decide
            self.logger.warning(
                f"Existing file found but no extraction log - cannot verify if data matches: {output_path}"
            )
            return False, "no extraction log found"

        # Compare extraction parameters
        log_info = extraction_log.get("extraction_info", {})

        # Check if key parameters match
        params_match = (
            log_info.get("pmu_id") == request.pmu_id
            and log_info.get("resolution") == request.resolution
            and log_info.get("start_date") == request.date_range.start.isoformat()
            and log_info.get("end_date") == request.date_range.end.isoformat()
            and log_info.get("processed") == request.processed
            and log_info.get("clean") == request.clean
            and log_info.get("output_format") == request.output_format
        )

        if not params_match:
            # Parameters don't match
            self.logger.warning(f"Existing file found but parameters don't match: {output_path}")

        return params_match, "exact match found" if params_match else "parameters don't match"

    def _initialise_log(self, request: ExtractionRequest) -> dict:
        # Get timezone information for the request period
        local_tz = self._get_local_timezone()
        start_offset = self._get_utc_offset(request.date_range.start, local_tz)
        end_offset = self._get_utc_offset(request.date_range.end, local_tz)

        return {
            "extraction_info": {
                "timestamp": datetime.now().isoformat(),
                "pmu_id": request.pmu_id,
                "resolution": request.resolution,
                "start_date": request.date_range.start.isoformat(),
                "end_date": request.date_range.end.isoformat(),
                "processed": request.processed,
                "clean": request.clean,
                "output_format": request.output_format,
                "timezone": str(local_tz) if local_tz else "UTC",
                "utc_offset_start": start_offset,
                "utc_offset_end": end_offset,
            },
            "data_quality": {},
            "column_changes": {"removed": [], "renamed": [], "added": [], "type_conversions": []},
            "issues_found": [],
            "statistics": {},
        }

    def _print_summary(self, df: pd.DataFrame) -> None:
        print("\n[DATA] Data Summary:")
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
        if "ts" in df.columns:
            print(f"   Local time range: {df['ts'].min()} to {df['ts'].max()}")
            if "ts_utc" in df.columns:
                print(f"   UTC time range: {df['ts_utc'].min()} to {df['ts_utc'].max()}")

    def _persist_dataframe(
        self, request: ExtractionRequest, df: pd.DataFrame, extraction_log: dict
    ):
        output_path = self._resolve_output_path(request).with_suffix(f".{request.output_format}")
        file_size_mb = self._write_output(df, output_path, request.output_format)

        extraction_log["statistics"]["final_rows"] = len(df)
        extraction_log["statistics"]["final_columns"] = len(df.columns)
        extraction_log["statistics"]["final_column_names"] = list(df.columns)
        extraction_log["statistics"]["rows_removed"] = extraction_log["statistics"][
            "original_rows"
        ] - len(df)
        extraction_log["statistics"]["file_size_mb"] = round(file_size_mb, 2)
        extraction_log["statistics"]["file_format"] = f".{request.output_format}"

        try:
            self._write_extraction_log(extraction_log, output_path)
        except Exception as exc:
            self.logger.warning("Could not write extraction log: %s", exc)

        self._print_summary(df)
        return output_path, file_size_mb

    def finalise(self, request: ExtractionRequest, df: pd.DataFrame, extraction_log: dict):
        return self._persist_dataframe(request, df, extraction_log)

    # ---------------------------------------------------------------- Extraction
    def extract(  # noqa: PLR0911 - Multiple returns for error handling and early exits
        self, request: ExtractionRequest, *, chunk_strategy: ChunkStrategy | None = None
    ) -> ExtractionResult:
        start_clock = time.monotonic()
        request.validate()

        # Resolve output path early to check for existing files
        output_path = self._resolve_output_path(request).with_suffix(f".{request.output_format}")

        # Check if we should skip based on existing file
        should_skip, _reason = self._check_existing_file(request, output_path)
        if should_skip:
            self.logger.info(f"Skipping extraction - file already exists: {output_path}")
            print(f"\n[SKIP] Output file already exists with matching parameters: {output_path}")
            print("[SKIP] Use --replace flag to overwrite existing files")

            # Try to read existing file stats
            existing_stats = {}
            try:
                extraction_log = self._read_extraction_log(output_path)
                if extraction_log:
                    existing_stats = extraction_log.get("statistics", {})
                    rows = existing_stats.get("final_rows", "unknown")
                    file_size = existing_stats.get("file_size_mb", "unknown")
                    print(f"[SKIP] Existing file contains: {rows} rows, {file_size} MB")
            except Exception:
                pass

            duration = time.monotonic() - start_clock
            return ExtractionResult(
                request=request,
                success=True,
                output_file=output_path,
                rows_extracted=existing_stats.get("final_rows", 0),
                extraction_time_seconds=duration,
                file_size_mb=existing_stats.get("file_size_mb", None),
                error=None,
            )

        extraction_log = self._initialise_log(request)
        df = self.data_extractor.extract(request, chunk_strategy=chunk_strategy)
        if df is None:
            duration = time.monotonic() - start_clock
            return ExtractionResult(
                request=request,
                success=False,
                output_file=None,
                rows_extracted=0,
                extraction_time_seconds=duration,
                error="Extraction returned no data",
            )

        extraction_log["statistics"]["original_rows"] = len(df)
        extraction_log["statistics"]["original_columns"] = len(df.columns)
        extraction_log["statistics"]["original_column_names"] = list(df.columns)

        if request.clean or request.processed:
            df, _ = self.data_processor.process(
                df,
                extraction_log=extraction_log,
                clean=request.clean,
                validate=request.clean,
            )
            if df is None:
                duration = time.monotonic() - start_clock
                return ExtractionResult(
                    request=request,
                    success=False,
                    output_file=None,
                    rows_extracted=0,
                    extraction_time_seconds=duration,
                    error="Data processing returned no data",
                )

        if request.processed and df is not None:
            df, _ = self.power_calculator.process_phasor_data(df, extraction_log=extraction_log)
            if df is None:
                duration = time.monotonic() - start_clock
                return ExtractionResult(
                    request=request,
                    success=False,
                    output_file=None,
                    rows_extracted=0,
                    extraction_time_seconds=duration,
                    error="Power calculation returned no data",
                )

        if df is None or len(df) == 0:
            duration = time.monotonic() - start_clock
            return ExtractionResult(
                request=request,
                success=False,
                output_file=None,
                rows_extracted=0,
                extraction_time_seconds=duration,
                error="No data available after processing",
            )

        try:
            output_path, file_size_mb = self._persist_dataframe(request, df, extraction_log)
        except Exception as exc:
            duration = time.monotonic() - start_clock
            self.logger.error("Failed to save output: %s", exc)
            return ExtractionResult(
                request=request,
                success=False,
                output_file=None,
                rows_extracted=len(df),
                extraction_time_seconds=duration,
                error=str(exc),
            )

        duration = time.monotonic() - start_clock

        return ExtractionResult(
            request=request,
            success=True,
            output_file=output_path,
            rows_extracted=len(df),
            extraction_time_seconds=duration,
            file_size_mb=round(file_size_mb, 2),
        )

    def batch_extract(  # noqa: PLR0912, PLR0915
        self,
        requests: list[ExtractionRequest],
        output_dir: Path | None = None,
        *,
        chunk_strategy: ChunkStrategy | None = None,
    ) -> BatchExtractionResult:
        """
        Extract data from multiple PMUs with consistent timeframe.

        Args:
            requests: List of extraction requests
            output_dir: Output directory path (optional)
            chunk_strategy: Optional chunking strategy to apply to all extractions

        Returns:
            BatchExtractionResult with aggregated outcomes
        """
        from .signal_handler import get_cancellation_manager  # noqa: PLC0415

        cancellation_manager = get_cancellation_manager()
        batch_start = datetime.now()
        batch_id = batch_start.strftime("%Y%m%d_%H%M%S")

        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
        else:
            config = self._config()
            if config and "output" in config:
                default_dir = config["output"].get("default_output_dir", "data_exports")
            else:
                default_dir = "data_exports"
            output_dir = Path(default_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info("Batch extraction for %d requests", len(requests))
        self.logger.info("Output directory: %s", output_dir)
        print("=" * 60)

        results = []

        for i, request in enumerate(requests, 1):
            # Check for cancellation before processing each PMU
            if cancellation_manager.is_cancelled():
                self.logger.warning(
                    f"Batch extraction cancelled after {i - 1}/{len(requests)} PMUs processed"
                )
                # Add cancelled results for remaining requests
                for remaining_request in requests[i - 1 :]:
                    cancelled_result = ExtractionResult(
                        request=remaining_request,
                        success=False,
                        output_file=None,
                        rows_extracted=0,
                        extraction_time_seconds=0.0,
                        error="Extraction cancelled by user",
                    )
                    results.append(cancelled_result)
                break

            self.logger.info("Processing PMU %d (%d/%d)", request.pmu_id, i, len(requests))

            if not request.output_file and output_dir:
                station_name = self._get_station_name(request.pmu_id)
                start_str = request.date_range.start.strftime("%Y%m%d_%H%M%S")
                end_str = request.date_range.end.strftime("%Y%m%d_%H%M%S")
                filename = f"pmu_{request.pmu_id}_{station_name}_{request.resolution}hz_{start_str}_to_{end_str}.{request.output_format}"
                request.output_file = output_dir / filename

            try:
                result = self.extract(request, chunk_strategy=chunk_strategy)
                results.append(result)
            except Exception as exc:
                self.logger.error("Error processing PMU %d: %s", request.pmu_id, exc)
                error_result = ExtractionResult(
                    request=request,
                    success=False,
                    output_file=None,
                    rows_extracted=0,
                    extraction_time_seconds=0.0,
                    error=str(exc),
                )
                results.append(error_result)

        batch_end = datetime.now()

        print("\n" + "=" * 60)
        print("[DATA] Batch Extraction Summary")
        print("=" * 60)

        batch_result = BatchExtractionResult(
            batch_id=batch_id,
            results=results,
            started_at=batch_start,
            finished_at=batch_end,
        )

        successful = batch_result.successful_results()
        failed = batch_result.failed_results()

        # Check if operation was cancelled
        if cancellation_manager.is_cancelled():
            cancelled_count = sum(
                1 for result in results if result.error == "Extraction cancelled by user"
            )
            self.logger.info(
                "Batch extraction cancelled: %d/%d successful, %d/%d failed, %d/%d cancelled",
                len(successful),
                len(requests),
                len(failed),
                len(requests),
                cancelled_count,
                len(requests),
            )
        else:
            self.logger.info(
                "Batch extraction completed: %d/%d successful, %d/%d failed",
                len(successful),
                len(requests),
                len(failed),
                len(requests),
            )

        if successful:
            self.logger.info("Successfully extracted:")
            for result in successful:
                print(f"   PMU {result.request.pmu_id}: {result.output_file}")

        if failed:
            self.logger.error("Failed extractions:")
            for result in failed:
                print(f"   PMU {result.request.pmu_id}: {result.error}")

        return batch_result
