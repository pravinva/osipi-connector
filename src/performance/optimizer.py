"""
Performance Optimization Utilities for Large-Scale PI Ingestion

Provides optimizations for handling 100K+ tags:
- Intelligent batch sizing based on data characteristics
- Parallel processing with connection pooling
- Memory-efficient chunking
- Adaptive rate limiting
"""

import logging
from typing import List, Dict, Any, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)


class PerformanceOptimizer:
    """
    Optimization engine for large-scale PI data ingestion.

    Dynamically adjusts batch sizes, parallelism, and processing
    based on system performance and data characteristics.
    """

    def __init__(
        self,
        base_batch_size: int = 100,
        max_batch_size: int = 200,
        min_batch_size: int = 25,
        max_workers: int = 10,
        adaptive_sizing: bool = True
    ):
        """
        Initialize performance optimizer.

        Args:
            base_batch_size: Starting batch size for tags
            max_batch_size: Maximum tags per batch
            min_batch_size: Minimum tags per batch
            max_workers: Maximum parallel workers
            adaptive_sizing: Enable adaptive batch size adjustment
        """
        self.base_batch_size = base_batch_size
        self.max_batch_size = max_batch_size
        self.min_batch_size = min_batch_size
        self.max_workers = max_workers
        self.adaptive_sizing = adaptive_sizing

        # Performance tracking
        self.batch_times: List[float] = []
        self.current_batch_size = base_batch_size
        self.total_processed = 0
        self.start_time = None

    def chunk_tags(
        self,
        tags: List[str],
        chunk_size: int = None
    ) -> Iterator[List[str]]:
        """
        Split tags into optimally-sized chunks.

        Args:
            tags: List of tag WebIDs
            chunk_size: Override chunk size (uses adaptive if None)

        Yields:
            Chunks of tag WebIDs
        """
        chunk_size = chunk_size or self.current_batch_size

        for i in range(0, len(tags), chunk_size):
            yield tags[i:i + chunk_size]

    def adjust_batch_size(self, batch_time: float, batch_size: int):
        """
        Dynamically adjust batch size based on performance.

        Args:
            batch_time: Time taken to process last batch (seconds)
            batch_size: Size of last batch processed
        """
        if not self.adaptive_sizing:
            return

        self.batch_times.append(batch_time)

        # Keep last 10 batch times
        if len(self.batch_times) > 10:
            self.batch_times.pop(0)

        # Calculate average batch time
        avg_time = sum(self.batch_times) / len(self.batch_times)

        # Target: 5-15 seconds per batch
        target_min = 5.0
        target_max = 15.0

        if avg_time < target_min:
            # Batches too fast - increase size
            new_size = min(
                int(batch_size * 1.2),
                self.max_batch_size
            )
            if new_size != self.current_batch_size:
                logger.info(
                    f"Increasing batch size: {self.current_batch_size} → {new_size} "
                    f"(avg time: {avg_time:.2f}s)"
                )
                self.current_batch_size = new_size

        elif avg_time > target_max:
            # Batches too slow - decrease size
            new_size = max(
                int(batch_size * 0.8),
                self.min_batch_size
            )
            if new_size != self.current_batch_size:
                logger.info(
                    f"Decreasing batch size: {self.current_batch_size} → {new_size} "
                    f"(avg time: {avg_time:.2f}s)"
                )
                self.current_batch_size = new_size

    def process_in_parallel(
        self,
        items: List[Any],
        process_func: callable,
        description: str = "Processing"
    ) -> List[Any]:
        """
        Process items in parallel with optimal worker count.

        Args:
            items: Items to process
            process_func: Function to apply to each item
            description: Description for logging

        Returns:
            List of processed results
        """
        logger.info(
            f"{description}: {len(items)} items with {self.max_workers} workers"
        )

        results = []
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(process_func, item): item
                for item in items
            }

            # Collect results as they complete
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    item = futures[future]
                    logger.error(f"Error processing {item}: {e}")

        elapsed = time.time() - start_time
        logger.info(
            f"{description} complete: {len(results)}/{len(items)} succeeded "
            f"in {elapsed:.2f}s ({len(results)/elapsed:.1f} items/sec)"
        )

        return results

    def estimate_completion_time(
        self,
        total_items: int,
        processed_items: int
    ) -> Dict[str, Any]:
        """
        Estimate remaining time based on current performance.

        Args:
            total_items: Total number of items to process
            processed_items: Number of items processed so far

        Returns:
            Dictionary with time estimates
        """
        if processed_items == 0 or not self.start_time:
            return {
                'eta_seconds': None,
                'eta_formatted': 'Calculating...',
                'percent_complete': 0
            }

        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = processed_items / elapsed
        remaining = total_items - processed_items
        eta_seconds = remaining / rate if rate > 0 else 0

        return {
            'eta_seconds': eta_seconds,
            'eta_formatted': str(timedelta(seconds=int(eta_seconds))),
            'percent_complete': (processed_items / total_items) * 100,
            'processing_rate': rate,
            'elapsed_seconds': elapsed
        }

    def start_tracking(self):
        """Start performance tracking."""
        self.start_time = datetime.now()
        self.total_processed = 0
        logger.info("Performance tracking started")

    def track_progress(self, items_processed: int):
        """
        Track processing progress.

        Args:
            items_processed: Number of items processed in this batch
        """
        self.total_processed += items_processed


class MemoryEfficientIterator:
    """
    Iterator for processing large datasets without loading everything into memory.

    Uses generator pattern to yield batches on-demand.
    """

    def __init__(
        self,
        tags: List[str],
        start_time: datetime,
        end_time: datetime,
        batch_size: int = 100
    ):
        """
        Initialize memory-efficient iterator.

        Args:
            tags: List of tag WebIDs
            start_time: Start time for data extraction
            end_time: End time for data extraction
            batch_size: Tags per batch
        """
        self.tags = tags
        self.start_time = start_time
        self.end_time = end_time
        self.batch_size = batch_size
        self.current_index = 0

    def __iter__(self):
        """Return iterator object."""
        self.current_index = 0
        return self

    def __next__(self) -> Dict[str, Any]:
        """
        Get next batch configuration.

        Returns:
            Dictionary with batch configuration

        Raises:
            StopIteration when all batches processed
        """
        if self.current_index >= len(self.tags):
            raise StopIteration

        # Get next batch of tags
        end_index = min(
            self.current_index + self.batch_size,
            len(self.tags)
        )
        batch_tags = self.tags[self.current_index:end_index]

        batch_config = {
            'tags': batch_tags,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'batch_number': (self.current_index // self.batch_size) + 1,
            'total_batches': (len(self.tags) + self.batch_size - 1) // self.batch_size
        }

        self.current_index = end_index

        return batch_config


class RateLimiter:
    """
    Adaptive rate limiter to prevent overwhelming PI server.

    Tracks API response times and adjusts request rate accordingly.
    """

    def __init__(
        self,
        initial_rate: int = 10,
        max_rate: int = 50,
        min_rate: int = 1
    ):
        """
        Initialize rate limiter.

        Args:
            initial_rate: Starting requests per second
            max_rate: Maximum requests per second
            min_rate: Minimum requests per second
        """
        self.current_rate = initial_rate
        self.max_rate = max_rate
        self.min_rate = min_rate
        self.last_request_time = None
        self.response_times: List[float] = []

    def wait_if_needed(self):
        """Wait if necessary to maintain rate limit."""
        if self.last_request_time is None:
            self.last_request_time = time.time()
            return

        # Calculate minimum wait time
        min_interval = 1.0 / self.current_rate
        elapsed = time.time() - self.last_request_time

        if elapsed < min_interval:
            wait_time = min_interval - elapsed
            time.sleep(wait_time)

        self.last_request_time = time.time()

    def record_response_time(self, response_time: float):
        """
        Record API response time and adjust rate if needed.

        Args:
            response_time: Response time in seconds
        """
        self.response_times.append(response_time)

        # Keep last 20 response times
        if len(self.response_times) > 20:
            self.response_times.pop(0)

        # Calculate average response time
        avg_time = sum(self.response_times) / len(self.response_times)

        # Adjust rate based on response times
        if avg_time < 0.5:
            # Fast responses - increase rate
            self.current_rate = min(
                int(self.current_rate * 1.1),
                self.max_rate
            )
        elif avg_time > 2.0:
            # Slow responses - decrease rate
            self.current_rate = max(
                int(self.current_rate * 0.9),
                self.min_rate
            )


def calculate_optimal_batch_size(
    total_tags: int,
    available_memory_gb: float = 8.0,
    avg_records_per_tag: int = 3600
) -> int:
    """
    Calculate optimal batch size based on system resources.

    Args:
        total_tags: Total number of tags to process
        available_memory_gb: Available memory in GB
        avg_records_per_tag: Average records per tag

    Returns:
        Recommended batch size
    """
    # Assume ~1KB per record
    bytes_per_record = 1024
    available_bytes = available_memory_gb * 1024 * 1024 * 1024

    # Reserve 50% for overhead
    usable_bytes = available_bytes * 0.5

    # Calculate records per batch that fit in memory
    max_records = usable_bytes / bytes_per_record

    # Calculate tags per batch
    batch_size = int(max_records / avg_records_per_tag)

    # Clamp to reasonable range
    batch_size = max(25, min(batch_size, 200))

    logger.info(
        f"Calculated optimal batch size: {batch_size} tags "
        f"(total: {total_tags}, memory: {available_memory_gb}GB)"
    )

    return batch_size
