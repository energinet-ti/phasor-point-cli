"""
High-level extraction manager that coordinates data retrieval, processing, and persistence.
"""

from __future__ import annotations

import json
import os
import time
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
import tzlocal

from .chunk_strategy import ChunkStrategy
from .config_paths import ConfigPathManager
from .data_extractor import DataExtractor
from .data_processor import DataProcessor
from .data_validator import DataValidator
from .extraction_history import ExtractionHistory
from .file_utils import FileUtils
from .models import BatchExtractionResult, ExtractionRequest, ExtractionResult, PersistResult
from .power_calculator import PowerCalculator
from .progress_tracker import ProgressTracker


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
        extraction_history: ExtractionHistory | None = None,
        verbose_timing: bool = False,
    ) -> None:
        self.connection_pool = connection_pool
        self.config_manager = config_manager
        self.logger = logger
        self.verbose_timing = verbose_timing

        validator = None
        if data_processor is None:
            validator = DataValidator(logger=logger)

        self.data_processor = data_processor or DataProcessor(
            config_manager=config_manager, logger=logger, validator=validator
        )
        self.data_extractor = data_extractor or DataExtractor(
            connection_pool=connection_pool,
            logger=logger,
            extraction_history=extraction_history,
        )
        self.power_calculator = power_calculator or PowerCalculator(logger=logger)

        # Initialize extraction history
        if extraction_history is None:
            config_path_manager = ConfigPathManager()
            extraction_history = ExtractionHistory(config_path_manager, logger=logger)
            extraction_history.load_history()
        self.extraction_history = extraction_history

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
            except pytz.exceptions.UnknownTimeZoneError:
                warnings.warn(
                    f"Invalid timezone in TZ environment variable: '{tz_env}'. "
                    f"Falling back to system timezone. "
                    f"Use a valid IANA timezone name (e.g., 'Europe/Copenhagen').",
                    UserWarning,
                    stacklevel=3,
                )
        try:
            return tzlocal.get_localzone()
        except Exception:
            # Final fallback to UTC if tzlocal fails
            return pytz.UTC

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
        except Exception as e:
            warnings.warn(
                f"Failed to calculate UTC offset for datetime {dt} with timezone {local_tz}: {e}. "
                f"Defaulting to +00:00. Check timezone configuration.",
                UserWarning,
                stacklevel=3,
            )
            return "+00:00"

    def _get_station_name(self, pmu_id: int) -> str:
        """Get sanitized station name from PMU ID."""
        pmu_info = self.config_manager.get_pmu_info(pmu_id)
        station_name = pmu_info.station_name if pmu_info else "unknown"
        return FileUtils.sanitize_filename(station_name)

    def _expected_output_path(
        self, request: ExtractionRequest, output_dir: Path | None = None
    ) -> Path:
        """
        Build expected output path using user-provided time window strings.

        Args:
            request: Extraction request with filename strings in date_range
            output_dir: Optional output directory for batch mode

        Returns:
            Expected output path
        """
        station_name = self._get_station_name(request.pmu_id)
        start_str = request.date_range.filename_start_str
        end_str = request.date_range.filename_end_str
        filename = f"pmu_{request.pmu_id}_{station_name}_{request.resolution}hz_{start_str}_to_{end_str}.{request.output_format}"

        if output_dir:
            return output_dir / filename
        return Path(filename)

    def _resolve_output_path(
        self, request: ExtractionRequest, output_dir: Path | None = None
    ) -> Path:
        """Resolve output path from explicit file or expected path."""
        if request.output_file:
            return Path(request.output_file).with_suffix(f".{request.output_format}")
        return self._expected_output_path(request, output_dir)

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
        Check if output file exists and should be skipped.

        Returns:
            (should_skip, reason) - tuple indicating if extraction should be skipped and why
        """
        # If replace flag is set, never skip
        if request.replace:
            if output_path.exists():
                self.logger.info(f"Replacing existing file: {output_path}")
            return False, "replace flag set"

        # Check if file exists
        if not output_path.exists():
            return False, "file does not exist"

        # File exists and replace not set - skip
        return True, "file already exists"

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
            print(f"   UTC time range: {df['ts'].min()} to {df['ts'].max()}")
            if "ts_local" in df.columns:
                print(f"   Local time range: {df['ts_local'].min()} to {df['ts_local'].max()}")

    def _persist_dataframe(
        self,
        request: ExtractionRequest,
        df: pd.DataFrame,
        extraction_log: dict,
        output_dir: Path | None = None,
    ) -> PersistResult:
        # Use expected path based on user-provided time window
        output_path = self._resolve_output_path(request, output_dir)

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
        return PersistResult(output_path=output_path, file_size_mb=file_size_mb)

    def finalise(
        self,
        request: ExtractionRequest,
        df: pd.DataFrame,
        extraction_log: dict,
        output_dir: Path | None = None,
    ) -> PersistResult:
        return self._persist_dataframe(request, df, extraction_log, output_dir)

    # ----------------------------------------------------------- Helper Methods
    def _build_failure_result(
        self, request: ExtractionRequest, duration: float, error: str, rows: int = 0
    ) -> ExtractionResult:
        """Build a failure ExtractionResult with common fields."""
        return ExtractionResult(
            request=request,
            success=False,
            output_file=None,
            rows_extracted=rows,
            extraction_time_seconds=duration,
            error=error,
        )

    def _handle_skip_existing_file(
        self, request: ExtractionRequest, output_path: Path, start_clock: float
    ) -> ExtractionResult | None:
        """
        Check if extraction should be skipped due to existing file.

        Returns:
            ExtractionResult if should skip, None if should continue
        """
        should_skip, _reason = self._check_existing_file(request, output_path)
        if not should_skip:
            return None

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

    def _setup_progress_tracker(
        self, request: ExtractionRequest, chunk_strategy: ChunkStrategy | None
    ) -> tuple[ProgressTracker | None, ChunkStrategy, bool]:
        """
        Setup progress tracker for chunked extractions.

        Returns:
            Tuple of (progress_tracker, strategy, use_chunking)
        """
        strategy = chunk_strategy or ChunkStrategy(
            chunk_size_minutes=request.chunk_size_minutes, logger=self.logger
        )
        use_chunking, chunks = strategy.should_use_chunking(
            request.date_range.start, request.date_range.end
        )

        progress_tracker = None
        if use_chunking and len(chunks) > 1:
            progress_tracker = ProgressTracker(
                extraction_history=self.extraction_history,
                verbose_timing=self.verbose_timing,
                logger=self.logger,
            )
            progress_tracker.start_extraction(
                total_chunks=len(chunks),
                pmu_id=request.pmu_id,
                estimated_rows=0,
            )

        return progress_tracker, strategy, use_chunking

    def _process_and_calculate(
        self,
        df: pd.DataFrame,
        request: ExtractionRequest,
        extraction_log: dict,
        start_clock: float,
    ) -> tuple[pd.DataFrame | None, ExtractionResult | None]:
        """
        Process data and calculate power values.

        Returns:
            Tuple of (processed_df, failure_result).
            If failure_result is not None, extraction should return early.
        """
        result_df: pd.DataFrame | None = df
        if request.clean or request.processed:
            result_df, _ = self.data_processor.process(
                df,
                extraction_log=extraction_log,
                clean=request.clean,
                validate=request.clean,
            )
            if result_df is None:
                duration = time.monotonic() - start_clock
                return None, self._build_failure_result(
                    request, duration, "Data processing returned no data"
                )

        if request.processed and result_df is not None:
            result_df, _ = self.power_calculator.process_phasor_data(
                result_df, extraction_log=extraction_log
            )
            if result_df is None:
                duration = time.monotonic() - start_clock
                return None, self._build_failure_result(
                    request, duration, "Power calculation returned no data"
                )

        return result_df, None

    def _resolve_batch_output_dir(self, output_dir: Path | None) -> Path:
        """Resolve and create output directory for batch extraction."""
        if output_dir:
            resolved_dir = Path(output_dir)
        else:
            config = self._config()
            if config and "output" in config:
                default_dir = config["output"].get("default_output_dir", "data_exports")
            else:
                default_dir = "data_exports"
            resolved_dir = Path(default_dir)

        resolved_dir.mkdir(parents=True, exist_ok=True)
        return resolved_dir

    def _handle_batch_cancellation(
        self, requests: list[ExtractionRequest], current_index: int
    ) -> list[ExtractionResult]:
        """Create cancelled results for remaining unprocessed requests."""
        cancelled_results = []
        for remaining_request in requests[current_index:]:
            cancelled_result = ExtractionResult(
                request=remaining_request,
                success=False,
                output_file=None,
                rows_extracted=0,
                extraction_time_seconds=0.0,
                error="Extraction cancelled by user",
            )
            cancelled_results.append(cancelled_result)
        return cancelled_results

    def _print_batch_summary(
        self, batch_result: BatchExtractionResult, cancellation_manager
    ) -> None:
        """Print batch extraction summary."""
        print("\n" + "=" * 60)
        print("[DATA] Batch Extraction Summary")
        print("=" * 60)

        successful = batch_result.successful_results()
        failed = batch_result.failed_results()
        total_requests = len(batch_result.results)

        # Check if operation was cancelled
        if cancellation_manager.is_cancelled():
            cancelled_count = sum(
                1
                for result in batch_result.results
                if result.error == "Extraction cancelled by user"
            )
            self.logger.info(
                "Batch extraction cancelled: %d/%d successful, %d/%d failed, %d/%d cancelled",
                len(successful),
                total_requests,
                len(failed),
                total_requests,
                cancelled_count,
                total_requests,
            )
        else:
            self.logger.info(
                "Batch extraction completed: %d/%d successful, %d/%d failed",
                len(successful),
                total_requests,
                len(failed),
                total_requests,
            )

        if successful:
            self.logger.info("Successfully extracted:")
            for result in successful:
                print(f"   PMU {result.request.pmu_id}: {result.output_file}")

        if failed:
            self.logger.error("Failed extractions:")
            for result in failed:
                print(f"   PMU {result.request.pmu_id}: {result.error}")

    # ---------------------------------------------------------------- Extraction
    def extract(  # noqa: PLR0911
        self,
        request: ExtractionRequest,
        *,
        chunk_strategy: ChunkStrategy | None = None,
        output_dir: Path | None = None,
    ) -> ExtractionResult:
        start_clock = time.monotonic()
        request.validate()

        # Determine expected output path early using user-provided time window
        expected_path = self._resolve_output_path(request, output_dir)

        # Check if we should skip based on existing file
        skip_result = self._handle_skip_existing_file(request, expected_path, start_clock)
        if skip_result:
            return skip_result

        extraction_log = self._initialise_log(request)

        # Setup progress tracking for chunked extractions
        progress_tracker, strategy, _use_chunking = self._setup_progress_tracker(
            request, chunk_strategy
        )

        # Extract data
        df = self.data_extractor.extract(
            request, chunk_strategy=strategy, progress_tracker=progress_tracker
        )

        # Finish progress display if used
        if progress_tracker:
            progress_tracker.finish_extraction()

        # Validate extracted data
        if df is None:
            duration = time.monotonic() - start_clock
            return self._build_failure_result(request, duration, "Extraction returned no data")

        extraction_log["statistics"]["original_rows"] = len(df)
        extraction_log["statistics"]["original_columns"] = len(df.columns)
        extraction_log["statistics"]["original_column_names"] = list(df.columns)

        # Process and calculate
        df, failure_result = self._process_and_calculate(df, request, extraction_log, start_clock)
        if failure_result:
            return failure_result

        # Validate processed data
        if df is None or len(df) == 0:
            duration = time.monotonic() - start_clock
            return self._build_failure_result(
                request, duration, "No data available after processing"
            )

        # Persist data
        try:
            result = self._persist_dataframe(request, df, extraction_log, output_dir)
            if result.skip_result:
                return result.skip_result
            output_path = result.output_path
            file_size_mb = result.file_size_mb
        except Exception as exc:
            duration = time.monotonic() - start_clock
            self.logger.error("Failed to save output: %s", exc)
            return self._build_failure_result(request, duration, str(exc), rows=len(df))

        duration = time.monotonic() - start_clock

        # Save extraction metrics to history for future time estimates
        if duration > 0 and len(df) > 0:
            self.extraction_history.add_extraction(
                rows=len(df),
                duration_sec=duration,
                chunk_size_minutes=request.chunk_size_minutes,
                parallel_workers=request.parallel_workers,
            )

        # Flush any pending chunk timings to disk
        self.extraction_history.flush()

        return ExtractionResult(
            request=request,
            success=True,
            output_file=output_path,
            rows_extracted=len(df),
            extraction_time_seconds=duration,
            file_size_mb=round(file_size_mb, 2),
        )

    def batch_extract(
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

        # Resolve output directory
        output_dir = self._resolve_batch_output_dir(output_dir)

        self.logger.info("Batch extraction for %d requests", len(requests))
        self.logger.info("Output directory: %s", output_dir)
        print("=" * 60)

        # Initialize batch progress tracker
        batch_progress = ProgressTracker(
            extraction_history=self.extraction_history,
            verbose_timing=self.verbose_timing,
            logger=self.logger,
        )
        batch_progress.start_batch(len(requests))

        results = []

        for i, request in enumerate(requests, 1):
            # Check for cancellation before processing each PMU
            if cancellation_manager.is_cancelled():
                self.logger.warning(
                    f"Batch extraction cancelled after {i - 1}/{len(requests)} PMUs processed"
                )
                # Add cancelled results for remaining requests
                cancelled_results = self._handle_batch_cancellation(requests, i - 1)
                results.extend(cancelled_results)
                break

            self.logger.info("Processing PMU %d (%d/%d)", request.pmu_id, i, len(requests))

            try:
                result = self.extract(request, chunk_strategy=chunk_strategy, output_dir=output_dir)
                results.append(result)

                # Update batch progress after PMU completes
                batch_progress.update_pmu_progress(i - 1, request.pmu_id)
            except Exception as exc:
                self.logger.error("Error processing PMU %d: %s", request.pmu_id, exc)
                error_result = self._build_failure_result(request, 0.0, str(exc))
                results.append(error_result)

        batch_end = datetime.now()

        # Finish batch progress tracking
        batch_progress.finish_batch()

        # Build batch result
        batch_result = BatchExtractionResult(
            batch_id=batch_id,
            results=results,
            started_at=batch_start,
            finished_at=batch_end,
        )

        # Print summary
        self._print_batch_summary(batch_result, cancellation_manager)

        return batch_result
