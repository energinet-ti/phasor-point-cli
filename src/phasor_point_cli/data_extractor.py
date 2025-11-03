"""
Object oriented data extraction utilities built on top of the existing
connection pool.
"""

from __future__ import annotations

import time
import warnings
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from .chunk_strategy import ChunkStrategy
from .constants import CLI_COMMAND_PYTHON
from .models import ExtractionRequest


class DataExtractor:
    """Perform single or chunked extractions using a database connection pool."""

    def __init__(
        self,
        connection_pool,
        logger,
        chunk_strategy: ChunkStrategy | None = None,
        extraction_history=None,
    ):
        self.connection_pool = connection_pool
        self.logger = logger
        self.chunk_strategy = chunk_strategy
        self.extraction_history = extraction_history

    # ------------------------------------------------------------------ Helpers
    def _ensure_strategy(self, chunk_size_minutes: int) -> ChunkStrategy:
        if self.chunk_strategy and self.chunk_strategy.chunk_size_minutes == chunk_size_minutes:
            return self.chunk_strategy
        self.chunk_strategy = ChunkStrategy(
            chunk_size_minutes=chunk_size_minutes, logger=self.logger
        )
        return self.chunk_strategy

    def _read_dataframe(self, conn, query: str) -> pd.DataFrame | None:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")
            return pd.read_sql(query, conn)

    def _build_query(self, table_name: str, start: str, end: str) -> str:
        return f"""
        SELECT *
        FROM {table_name}
        WHERE ts >= '{start}' AND ts < '{end}'
        ORDER BY ts
        """

    # ---------------------------------------------------------- Public methods
    def extract_single(
        self, table_name: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """Extract data using a single query."""
        conn = self.connection_pool.get_connection()
        if not conn:
            return None

        try:
            query = self._build_query(table_name, start_date, end_date)
            self.logger.info("Executing query...")
            df = self._read_dataframe(conn, query)

            if df is None:
                self.logger.error("Query returned None")
                return None

            if len(df) == 0:
                self.logger.error("No data found for the specified date range")
                print(
                    f"[TIP] Use '{CLI_COMMAND_PYTHON} table-info --pmu <pmu_id>' to check available date range"
                )
                return None

            self.logger.info(f"Retrieved {len(df):,} rows with {len(df.columns)} columns")
            return df
        except Exception as exc:
            self.logger.error(f"Error extracting data: {exc}")
            import traceback  # noqa: PLC0415 - only needed on error path

            self.logger.debug(f"Extraction error details: {traceback.format_exc()}")
            return None
        finally:
            self.connection_pool.return_connection(conn)

    def extract_chunk_sequential(
        self,
        table_name: str,
        chunks: Sequence[tuple[pd.Timestamp, pd.Timestamp]],
        progress_tracker=None,
        chunk_size_minutes: int = 15,
    ) -> list[pd.DataFrame]:
        """Extract data for each chunk sequentially."""
        from .signal_handler import get_cancellation_manager  # noqa: PLC0415

        cancellation_manager = get_cancellation_manager()
        all_chunks: list[pd.DataFrame] = []

        for index, (chunk_start, chunk_end) in enumerate(chunks):
            # Check for cancellation before processing each chunk
            if cancellation_manager.is_cancelled():
                self.logger.warning(
                    f"Extraction cancelled after {index}/{len(chunks)} chunks processed"
                )
                break

            chunk_start_str = chunk_start.strftime("%Y-%m-%d %H:%M:%S")
            chunk_end_str = chunk_end.strftime("%Y-%m-%d %H:%M:%S")
            self.logger.debug(
                f"Processing chunk {index + 1}/{len(chunks)}: {chunk_start_str} to {chunk_end_str}"
            )

            conn = self.connection_pool.get_connection()
            if not conn:
                self.logger.error(f"Could not create connection for chunk {index + 1}")
                continue

            chunk_start_time = time.time()
            try:
                query = self._build_query(table_name, chunk_start_str, chunk_end_str)
                chunk_df = self._read_dataframe(conn, query)
                if chunk_df is not None and len(chunk_df) > 0:
                    all_chunks.append(chunk_df)

                    # Save timing metrics after successful chunk
                    chunk_duration = time.time() - chunk_start_time
                    if self.extraction_history and chunk_duration > 0:
                        self.extraction_history.add_extraction(
                            rows=len(chunk_df),
                            duration_sec=chunk_duration,
                            chunk_size_minutes=chunk_size_minutes,
                            parallel_workers=1,
                        )

                    if progress_tracker:
                        progress_tracker.update_chunk_progress(index, len(chunk_df))
                else:
                    self.logger.warning(f"No data found for chunk {index + 1}")
                    if progress_tracker:
                        progress_tracker.update_chunk_progress(index, 0)
            except Exception as exc:
                self.logger.error(f"Error processing chunk {index + 1}: {exc}")
            finally:
                self.connection_pool.return_connection(conn)

        return all_chunks

    def _extract_single_chunk(
        self, table_name: str, chunk_start: pd.Timestamp, chunk_end: pd.Timestamp, chunk_index: int
    ):
        """Helper used by parallel extraction."""
        timing_info = {}
        chunk_start_str = chunk_start.strftime("%Y-%m-%d %H:%M:%S")
        chunk_end_str = chunk_end.strftime("%Y-%m-%d %H:%M:%S")

        total_start = time.time()
        conn_start = time.time()
        conn = self.connection_pool.get_connection()
        timing_info["connection_time"] = time.time() - conn_start

        if not conn:
            return None, f"Could not create connection for chunk {chunk_index + 1}", timing_info

        try:
            query = self._build_query(table_name, chunk_start_str, chunk_end_str)
            query_start = time.time()
            chunk_df = self._read_dataframe(conn, query)
            timing_info["query_time"] = time.time() - query_start
            timing_info["total_time"] = time.time() - total_start

            if chunk_df is not None and len(chunk_df) > 0:
                return chunk_df, None, timing_info
            return None, f"No data found for chunk {chunk_index + 1}", timing_info
        except Exception as exc:
            timing_info["total_time"] = time.time() - total_start
            return None, f"Error processing chunk {chunk_index + 1}: {exc}", timing_info
        finally:
            self.connection_pool.return_connection(conn)

    def extract_chunk_with_timing(
        self,
        table_name: str,
        chunk_start: pd.Timestamp,
        chunk_end: pd.Timestamp,
        chunk_index: int,
    ):
        """Public wrapper returning dataframe, error message and timing info."""
        return self._extract_single_chunk(table_name, chunk_start, chunk_end, chunk_index)

    def extract_chunk_parallel(
        self,
        table_name: str,
        chunks: Sequence[tuple[pd.Timestamp, pd.Timestamp]],
        parallel_workers: int,
        progress_tracker=None,
        chunk_size_minutes: int = 15,
    ) -> list[tuple[int, pd.DataFrame]]:
        """Extract data chunks using a thread pool."""
        from .signal_handler import get_cancellation_manager  # noqa: PLC0415

        cancellation_manager = get_cancellation_manager()
        results: list[tuple[int, pd.DataFrame]] = []
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            future_to_index = {
                executor.submit(
                    self._extract_single_chunk, table_name, chunk_start, chunk_end, idx
                ): idx
                for idx, (chunk_start, chunk_end) in enumerate(chunks)
            }

            for future in as_completed(future_to_index):
                # Check for cancellation before processing each completed future
                if cancellation_manager.is_cancelled():
                    self.logger.warning(
                        f"Extraction cancelled - {len(results)}/{len(chunks)} chunks completed"
                    )
                    # Cancel remaining futures
                    for f in future_to_index:
                        if not f.done():
                            f.cancel()
                    break

                idx = future_to_index[future]
                chunk_df, error, timing_info = future.result()
                if chunk_df is not None:
                    results.append((idx, chunk_df))

                    # Save timing metrics after successful chunk
                    chunk_duration = timing_info.get("total_time", 0)
                    if self.extraction_history and chunk_duration > 0:
                        self.extraction_history.add_extraction(
                            rows=len(chunk_df),
                            duration_sec=chunk_duration,
                            chunk_size_minutes=chunk_size_minutes,
                            parallel_workers=parallel_workers,
                        )

                    if progress_tracker:
                        progress_tracker.update_chunk_progress(idx, len(chunk_df))
                    self.logger.debug(
                        f"Chunk {idx + 1} completed ({len(chunk_df)} rows). Timing: {timing_info}"
                    )
                else:
                    self.logger.warning(f"Chunk {idx + 1} failed: {error}")
                    if progress_tracker:
                        progress_tracker.update_chunk_progress(idx, 0)
        return results

    def combine_chunks(self, chunks: Iterable) -> pd.DataFrame | None:
        """Combine chunk dataframes into a single dataframe."""
        chunk_list = [chunk for chunk in chunks if chunk is not None]
        if not chunk_list:
            self.logger.warning("No chunk data to combine")
            return None

        if isinstance(chunk_list[0], tuple):
            chunk_list = sorted(chunk_list, key=lambda item: item[0])
            chunk_frames = [
                item[1] for item in chunk_list if item[1] is not None and len(item[1]) > 0
            ]
        else:
            chunk_frames = [frame for frame in chunk_list if len(frame) > 0]

        if not chunk_frames:
            self.logger.warning("No chunk data to combine")
            return None

        combined = pd.concat(chunk_frames, ignore_index=True)
        if "ts" in combined.columns:
            combined = (
                combined.sort_values("ts").drop_duplicates(subset="ts").reset_index(drop=True)
            )
        self.logger.info(
            f"Combined {len(chunk_frames)} chunks into dataframe with {len(combined)} rows"
        )
        return combined

    def extract(
        self,
        request: ExtractionRequest,
        *,
        chunk_strategy: ChunkStrategy | None = None,
        progress_tracker=None,
    ) -> pd.DataFrame | None:
        """
        Main entry point that accepts an ``ExtractionRequest`` and returns a dataframe.
        """
        request.validate()

        table_name = f"pmu_{request.pmu_id}_{request.resolution}"
        start_dt, end_dt = request.date_range.as_database_time()
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

        strategy = chunk_strategy or self._ensure_strategy(request.chunk_size_minutes)
        use_chunking, chunks = strategy.should_use_chunking(start_dt, end_dt)

        if not use_chunking:
            return self.extract_single(table_name, start_str, end_str)

        # Pause progress display before logging (clears line and moves to new line)
        if progress_tracker:
            progress_tracker.pause_display()

        self.logger.info(
            "Using chunked extraction with chunk size %s minutes (%s chunks total)",
            strategy.chunk_size_minutes,
            len(chunks),
        )

        # Resume progress display after logging
        if progress_tracker:
            progress_tracker.resume_display()

        if request.parallel_workers > 1:
            chunk_frames = self.extract_chunk_parallel(
                table_name,
                chunks,
                request.parallel_workers,
                progress_tracker,
                strategy.chunk_size_minutes,
            )
        else:
            chunk_frames = self.extract_chunk_sequential(
                table_name, chunks, progress_tracker, strategy.chunk_size_minutes
            )

        return self.combine_chunks(chunk_frames)
