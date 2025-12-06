"""
Tests for Performance Optimizer Module

Tests performance optimization utilities for large-scale ingestion.
"""

import pytest
import time
from datetime import datetime, timedelta
from src.performance.optimizer import (
    PerformanceOptimizer,
    MemoryEfficientIterator,
    RateLimiter,
    calculate_optimal_batch_size
)


class TestPerformanceOptimizer:
    """Test PerformanceOptimizer class."""

    def test_initialization(self):
        """Test optimizer initialization."""
        optimizer = PerformanceOptimizer(
            base_batch_size=100,
            max_batch_size=200,
            min_batch_size=25
        )

        assert optimizer.base_batch_size == 100
        assert optimizer.max_batch_size == 200
        assert optimizer.min_batch_size == 25
        assert optimizer.current_batch_size == 100

    def test_chunk_tags(self):
        """Test tag chunking."""
        optimizer = PerformanceOptimizer(base_batch_size=10)
        tags = [f'TAG-{i:03d}' for i in range(25)]

        chunks = list(optimizer.chunk_tags(tags))

        assert len(chunks) == 3
        assert len(chunks[0]) == 10
        assert len(chunks[1]) == 10
        assert len(chunks[2]) == 5

    def test_chunk_tags_custom_size(self):
        """Test chunking with custom size."""
        optimizer = PerformanceOptimizer()
        tags = [f'TAG-{i:03d}' for i in range(50)]

        chunks = list(optimizer.chunk_tags(tags, chunk_size=20))

        assert len(chunks) == 3
        assert len(chunks[0]) == 20

    def test_adjust_batch_size_increase(self):
        """Test batch size increases when processing is fast."""
        optimizer = PerformanceOptimizer(
            base_batch_size=100,
            max_batch_size=200,
            adaptive_sizing=True
        )

        # Simulate fast batches (< 5 seconds)
        for _ in range(5):
            optimizer.adjust_batch_size(batch_time=3.0, batch_size=100)

        assert optimizer.current_batch_size > 100

    def test_adjust_batch_size_decrease(self):
        """Test batch size decreases when processing is slow."""
        optimizer = PerformanceOptimizer(
            base_batch_size=100,
            min_batch_size=25,
            adaptive_sizing=True
        )

        # Simulate slow batches (> 15 seconds)
        for _ in range(5):
            optimizer.adjust_batch_size(batch_time=20.0, batch_size=100)

        assert optimizer.current_batch_size < 100

    def test_adjust_batch_size_respects_limits(self):
        """Test batch size stays within min/max bounds."""
        optimizer = PerformanceOptimizer(
            base_batch_size=100,
            max_batch_size=150,
            min_batch_size=50,
            adaptive_sizing=True
        )

        # Try to increase beyond max
        for _ in range(10):
            optimizer.adjust_batch_size(batch_time=1.0, batch_size=optimizer.current_batch_size)

        assert optimizer.current_batch_size <= 150

        # Try to decrease below min
        for _ in range(10):
            optimizer.adjust_batch_size(batch_time=30.0, batch_size=optimizer.current_batch_size)

        assert optimizer.current_batch_size >= 50

    def test_adjust_batch_size_disabled(self):
        """Test batch size doesn't change when adaptive sizing disabled."""
        optimizer = PerformanceOptimizer(
            base_batch_size=100,
            adaptive_sizing=False
        )

        optimizer.adjust_batch_size(batch_time=1.0, batch_size=100)
        assert optimizer.current_batch_size == 100

        optimizer.adjust_batch_size(batch_time=30.0, batch_size=100)
        assert optimizer.current_batch_size == 100

    def test_estimate_completion_time_no_progress(self):
        """Test ETA when no progress made yet."""
        optimizer = PerformanceOptimizer()

        eta = optimizer.estimate_completion_time(total_items=1000, processed_items=0)

        assert eta['eta_seconds'] is None
        assert eta['percent_complete'] == 0

    def test_estimate_completion_time_with_progress(self):
        """Test ETA calculation with progress."""
        optimizer = PerformanceOptimizer()
        optimizer.start_tracking()

        # Simulate some processing time
        time.sleep(0.1)
        optimizer.track_progress(100)

        eta = optimizer.estimate_completion_time(total_items=1000, processed_items=100)

        assert eta['percent_complete'] == 10.0
        assert eta['eta_seconds'] is not None
        assert eta['processing_rate'] > 0

    def test_process_in_parallel(self):
        """Test parallel processing."""
        optimizer = PerformanceOptimizer(max_workers=4)

        def square(x):
            return x * x

        items = list(range(10))
        results = optimizer.process_in_parallel(
            items=items,
            process_func=square,
            description="Test processing"
        )

        assert len(results) == 10
        assert 25 in results  # 5^2


class TestMemoryEfficientIterator:
    """Test MemoryEfficientIterator class."""

    def test_iteration(self):
        """Test basic iteration."""
        tags = [f'TAG-{i:03d}' for i in range(25)]
        start_time = datetime(2025, 1, 1)
        end_time = datetime(2025, 1, 2)

        iterator = MemoryEfficientIterator(
            tags=tags,
            start_time=start_time,
            end_time=end_time,
            batch_size=10
        )

        batches = list(iterator)

        assert len(batches) == 3
        assert len(batches[0]['tags']) == 10
        assert batches[0]['batch_number'] == 1
        assert batches[0]['total_batches'] == 3

    def test_iteration_exact_division(self):
        """Test iteration when tags divide evenly into batches."""
        tags = [f'TAG-{i:03d}' for i in range(20)]

        iterator = MemoryEfficientIterator(
            tags=tags,
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 2),
            batch_size=10
        )

        batches = list(iterator)

        assert len(batches) == 2
        assert all(len(b['tags']) == 10 for b in batches)

    def test_multiple_iterations(self):
        """Test iterator can be used multiple times."""
        tags = [f'TAG-{i:03d}' for i in range(15)]

        iterator = MemoryEfficientIterator(
            tags=tags,
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 2),
            batch_size=5
        )

        # First iteration
        batches1 = list(iterator)
        # Second iteration
        batches2 = list(iterator)

        assert len(batches1) == len(batches2) == 3


class TestRateLimiter:
    """Test RateLimiter class."""

    def test_initialization(self):
        """Test rate limiter initialization."""
        limiter = RateLimiter(initial_rate=10, max_rate=50, min_rate=1)

        assert limiter.current_rate == 10
        assert limiter.max_rate == 50
        assert limiter.min_rate == 1

    def test_wait_if_needed(self):
        """Test rate limiting wait."""
        limiter = RateLimiter(initial_rate=10)  # 10 requests/second

        start = time.time()
        limiter.wait_if_needed()
        limiter.wait_if_needed()
        elapsed = time.time() - start

        # Should wait ~0.1 seconds between requests
        assert elapsed >= 0.08  # Allow some tolerance

    def test_record_response_time_increases_rate(self):
        """Test rate increases on fast responses."""
        limiter = RateLimiter(initial_rate=10, max_rate=50)
        initial_rate = limiter.current_rate

        # Simulate fast responses
        for _ in range(10):
            limiter.record_response_time(0.2)

        assert limiter.current_rate > initial_rate

    def test_record_response_time_decreases_rate(self):
        """Test rate decreases on slow responses."""
        limiter = RateLimiter(initial_rate=10, min_rate=1)
        initial_rate = limiter.current_rate

        # Simulate slow responses
        for _ in range(10):
            limiter.record_response_time(3.0)

        assert limiter.current_rate < initial_rate

    def test_rate_respects_limits(self):
        """Test rate stays within min/max bounds."""
        limiter = RateLimiter(initial_rate=10, max_rate=15, min_rate=5)

        # Try to increase beyond max
        for _ in range(20):
            limiter.record_response_time(0.1)

        assert limiter.current_rate <= 15

        # Try to decrease below min
        for _ in range(20):
            limiter.record_response_time(5.0)

        assert limiter.current_rate >= 5


class TestOptimalBatchSizeCalculation:
    """Test optimal batch size calculation."""

    def test_calculate_optimal_batch_size(self):
        """Test batch size calculation."""
        batch_size = calculate_optimal_batch_size(
            total_tags=10000,
            available_memory_gb=8.0,
            avg_records_per_tag=3600
        )

        assert 25 <= batch_size <= 200

    def test_calculate_optimal_batch_size_low_memory(self):
        """Test batch size with limited memory."""
        batch_size = calculate_optimal_batch_size(
            total_tags=10000,
            available_memory_gb=2.0,
            avg_records_per_tag=3600
        )

        assert batch_size >= 25

    def test_calculate_optimal_batch_size_high_memory(self):
        """Test batch size with abundant memory."""
        batch_size = calculate_optimal_batch_size(
            total_tags=10000,
            available_memory_gb=64.0,
            avg_records_per_tag=3600
        )

        assert batch_size <= 200

    def test_calculate_optimal_batch_size_high_records(self):
        """Test batch size with high record count per tag."""
        batch_size = calculate_optimal_batch_size(
            total_tags=10000,
            available_memory_gb=8.0,
            avg_records_per_tag=86400  # 1 day at 1 Hz
        )

        # Should recommend smaller batches for high record counts
        assert batch_size < 100
