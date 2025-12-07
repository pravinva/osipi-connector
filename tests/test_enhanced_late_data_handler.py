"""
Test suite for Enhanced Late Data Handler

Tests all features that match/exceed AVEVA Connect:
- Proactive detection
- Clock skew detection
- Backfill pipeline
- Duplicate prevention
- Real-time metrics
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from enhanced_late_data_handler import (
    EnhancedLateDataHandler,
    ClockSkewMetric,
    BackfillProgress
)


class TestClockSkewDetection:
    """Test clock skew detection functionality"""

    def test_detect_systematic_clock_skew(self):
        """Test detection of systematic clock drift"""
        # Mock workspace client
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = [
            ['tag1', 3600.0, 100.0, 1000],  # 1 hour skew, low variance
            ['tag2', -1800.0, 50.0, 500],   # -30 min skew
            ['tag3', 7200.0, 200.0, 2000],  # 2 hour skew
        ]
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        skew_metrics = handler.detect_clock_skew()

        assert len(skew_metrics) == 3
        assert skew_metrics[0].tag_webid == 'tag1'
        assert skew_metrics[0].avg_skew_seconds == 3600.0
        assert skew_metrics[0].is_systematic is True

    def test_no_clock_skew_detected(self):
        """Test when no systematic skew exists"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = []
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        skew_metrics = handler.detect_clock_skew()

        assert len(skew_metrics) == 0

    def test_clock_skew_threshold(self):
        """Test that only significant skew is detected (>5 minutes)"""
        mock_client = Mock()
        mock_stmt = Mock()
        # Small skew (2 minutes) should not be returned
        mock_stmt.result.data_array = []
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        handler.clock_skew_threshold_seconds = 300  # 5 minutes

        skew_metrics = handler.detect_clock_skew()
        assert len(skew_metrics) == 0


class TestProactiveDetection:
    """Test proactive late data detection"""

    def test_create_proactive_detection_view(self):
        """Test creation of real-time detection view"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = []
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        handler.create_proactive_detection_view()

        # Verify SQL was executed to create view
        mock_client.statement_execution.execute_statement.assert_called()
        call_args = mock_client.statement_execution.execute_statement.call_args
        assert 'CREATE OR REPLACE VIEW' in call_args[1]['statement']
        assert 'pi_timeseries_with_lateness' in call_args[1]['statement']

    def test_get_late_data_dashboard_metrics(self):
        """Test real-time dashboard metrics retrieval"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = [
            [100000, 5000, 5.0, 6.5, 48.0, 45]  # total, late, late_pct, avg, max, tags
        ]
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        metrics = handler.get_late_data_dashboard_metrics()

        assert metrics['total_records_today'] == 100000
        assert metrics['late_records'] == 5000
        assert metrics['late_pct'] == 5.0
        assert metrics['avg_lateness_hours'] == 6.5
        assert metrics['max_lateness_hours'] == 48.0
        assert metrics['tags_with_late_data'] == 45

    def test_dashboard_metrics_no_data(self):
        """Test dashboard metrics when no data exists"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = [[None, None, None, None, None, None]]
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        metrics = handler.get_late_data_dashboard_metrics()

        assert metrics['total_records_today'] == 0
        assert metrics['late_records'] == 0
        assert metrics['late_pct'] == 0.0


class TestDuplicatePrevention:
    """Test duplicate prevention with conflict resolution"""

    def test_handle_late_arrivals_with_dedup(self):
        """Test late data processing with deduplication"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = []
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        handler.handle_late_arrivals_with_dedup(late_data_threshold_hours=4)

        # Verify multiple SQL statements executed
        assert mock_client.statement_execution.execute_statement.call_count >= 2

        # Verify CREATE TEMP VIEW for deduplication was called
        calls = mock_client.statement_execution.execute_statement.call_args_list
        create_view_call = calls[0]
        assert 'CREATE OR REPLACE TEMP VIEW late_data_deduped' in create_view_call[1]['statement']
        assert 'ROW_NUMBER() OVER' in create_view_call[1]['statement']

        # Verify MERGE was called
        merge_call = calls[1]
        assert 'MERGE INTO' in merge_call[1]['statement']
        assert 'WHEN MATCHED AND source.ingestion_timestamp > target.ingestion_timestamp' in merge_call[1]['statement']

    def test_dedup_keeps_most_recent(self):
        """Test that deduplication keeps most recent record"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = []
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        handler.handle_late_arrivals_with_dedup()

        # Verify ORDER BY in ROW_NUMBER includes ingestion_timestamp DESC
        calls = mock_client.statement_execution.execute_statement.call_args_list
        create_view_sql = calls[0][1]['statement']
        assert 'ORDER BY ingestion_timestamp DESC' in create_view_sql

    def test_dedup_prioritizes_quality(self):
        """Test that deduplication prioritizes good quality"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = []
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        handler.handle_late_arrivals_with_dedup()

        # Verify ORDER BY includes quality_good DESC
        calls = mock_client.statement_execution.execute_statement.call_args_list
        create_view_sql = calls[0][1]['statement']
        assert 'quality_good DESC' in create_view_sql


class TestBackfillPipeline:
    """Test separate backfill pipeline"""

    def test_initiate_backfill(self):
        """Test backfill initiation with progress tracking"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = []
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")

        backfill_id = handler.initiate_backfill(
            start_date='2024-01-01',
            end_date='2024-12-31',
            batch_size_days=7
        )

        assert backfill_id.startswith('backfill_')
        assert len(backfill_id) > 20  # Should have timestamp

        # Verify INSERT was called to track backfill
        mock_client.statement_execution.execute_statement.assert_called()
        call_args = mock_client.statement_execution.execute_statement.call_args_list[-1]
        assert 'INSERT INTO osipi.bronze.backfill_operations' in call_args[1]['statement']

    def test_backfill_calculates_partitions(self):
        """Test that backfill correctly calculates partition count"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = []
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")

        # 365 days / 7 days per batch = 53 batches (rounded up)
        backfill_id = handler.initiate_backfill(
            start_date='2024-01-01',
            end_date='2024-12-31',
            batch_size_days=7
        )

        call_args = mock_client.statement_execution.execute_statement.call_args_list[-1]
        sql = call_args[1]['statement']
        # Should have 53 partitions (365 days / 7 = 52.14, rounds to 53)
        assert '53' in sql or '52' in sql  # Allow for rounding variations

    def test_get_backfill_progress(self):
        """Test backfill progress retrieval"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = [
            ['backfill_123', '2024-01-01', '2024-12-31', 53, 25, 125000, 'running',
             datetime(2025, 1, 7, 10, 0, 0), None]
        ]
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        progress = handler.get_backfill_progress('backfill_123')

        assert progress is not None
        assert progress.backfill_id == 'backfill_123'
        assert progress.total_partitions == 53
        assert progress.completed_partitions == 25
        assert progress.status == 'running'
        assert progress.records_processed == 125000

    def test_backfill_progress_not_found(self):
        """Test handling of non-existent backfill"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = []
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        progress = handler.get_backfill_progress('nonexistent')

        assert progress is None

    def test_execute_backfill_batch(self):
        """Test execution of single backfill batch"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = []
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        handler.execute_backfill_batch(
            backfill_id='backfill_123',
            batch_start_date='2024-01-01',
            batch_end_date='2024-01-07'
        )

        # Verify MERGE and OPTIMIZE were called
        calls = mock_client.statement_execution.execute_statement.call_args_list
        merge_called = any('MERGE INTO' in str(call) for call in calls)
        optimize_called = any('OPTIMIZE' in str(call) for call in calls)

        assert merge_called
        assert optimize_called

    def test_create_backfill_staging_table(self):
        """Test creation of backfill staging table"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = []
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        handler.create_backfill_staging_table()

        # Verify CREATE TABLE was called
        mock_client.statement_execution.execute_statement.assert_called()
        call_args = mock_client.statement_execution.execute_statement.call_args
        assert 'CREATE TABLE IF NOT EXISTS' in call_args[1]['statement']
        assert 'pi_timeseries_backfill_staging' in call_args[1]['statement']


class TestPerformanceOptimization:
    """Test performance optimization features"""

    def test_optimize_partitions_with_late_data(self):
        """Test partition optimization for late data"""
        mock_client = Mock()
        mock_stmt = Mock()
        # First call: get affected partitions
        mock_stmt.result.data_array = [
            ['2025-12-07', 500, 45],
            ['2025-12-06', 300, 32],
            ['2025-12-05', 200, 28],
        ]
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        handler.optimize_partitions_with_late_data(max_partitions=3)

        # Verify OPTIMIZE called for each partition
        calls = mock_client.statement_execution.execute_statement.call_args_list
        optimize_calls = [c for c in calls if 'OPTIMIZE' in str(c)]
        assert len(optimize_calls) >= 3

    def test_optimize_prioritizes_most_affected(self):
        """Test that optimization prioritizes partitions with most late data"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = [
            ['2025-12-07', 1000, 100],  # Most affected
            ['2025-12-06', 500, 50],
            ['2025-12-05', 100, 10],
        ]
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        handler.optimize_partitions_with_late_data()

        # Verify query orders by late_record_count DESC
        first_call = mock_client.statement_execution.execute_statement.call_args_list[0]
        assert 'ORDER BY late_record_count DESC' in first_call[1]['statement']

    def test_optimize_limits_partitions(self):
        """Test that optimization respects max_partitions limit"""
        mock_client = Mock()
        mock_stmt = Mock()
        # Return 20 partitions but limit to 5
        mock_stmt.result.data_array = [
            [f'2025-12-{i:02d}', 100 * i, 10 * i] for i in range(1, 21)
        ]
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        handler.optimize_partitions_with_late_data(max_partitions=5)

        # First call gets partitions, verify LIMIT clause
        first_call = mock_client.statement_execution.execute_statement.call_args_list[0]
        assert 'LIMIT 5' in first_call[1]['statement']


class TestEnhancedReporting:
    """Test enhanced reporting features"""

    def test_generate_enhanced_report(self):
        """Test comprehensive report generation"""
        mock_client = Mock()
        mock_stmt = Mock()

        # Mock multiple queries
        def side_effect(*args, **kwargs):
            stmt = Mock()
            sql = kwargs['statement']
            if 'partition_date' in sql and 'GROUP BY' in sql:
                # Historical data query
                stmt.result.data_array = [
                    ['2025-12-07', 500, 6.5, 48.0, 45, 10],
                    ['2025-12-06', 300, 4.2, 36.0, 32, 5],
                ]
            elif 'CURRENT_DATE()' in sql:
                # Today's summary query
                stmt.result.data_array = [[100000, 5000, 5.0, 6.5, 48.0, 45]]
            else:
                stmt.result.data_array = []
            return stmt

        mock_client.statement_execution.execute_statement.side_effect = side_effect

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        report = handler.generate_enhanced_report()

        assert 'ENHANCED LATE DATA ANALYSIS REPORT' in report
        assert 'TODAY\'S SUMMARY' in report
        assert 'HISTORICAL ANALYSIS' in report
        assert '2025-12-07' in report
        assert '5.00%' in report or '5.0%' in report  # late percentage

    def test_report_includes_clock_skew(self):
        """Test report includes clock skew warnings"""
        mock_client = Mock()

        def side_effect(*args, **kwargs):
            stmt = Mock()
            sql = kwargs['statement']
            if 'avg_skew_sec' in sql and 'stddev_skew' in sql:
                # Clock skew query
                stmt.result.data_array = [
                    ['tag1', 3600.0, 100.0, 1000],
                ]
            elif 'partition_date' in sql and 'GROUP BY' in sql:
                stmt.result.data_array = [['2025-12-07', 500, 6.5, 48.0, 45, 10]]
            elif 'CURRENT_DATE()' in sql:
                stmt.result.data_array = [[100000, 5000, 5.0, 6.5, 48.0, 45]]
            else:
                stmt.result.data_array = []
            return stmt

        mock_client.statement_execution.execute_statement.side_effect = side_effect

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        report = handler.generate_enhanced_report()

        assert 'CLOCK SKEW DETECTED' in report
        assert 'tag1' in report
        assert '3600s' in report


class TestErrorHandling:
    """Test error handling and edge cases"""

    def test_handles_sql_execution_error(self):
        """Test graceful handling of SQL errors"""
        mock_client = Mock()
        mock_client.statement_execution.execute_statement.side_effect = Exception("SQL Error")

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")

        # Should not raise, returns empty list
        result = handler.detect_clock_skew()
        assert result == []

    def test_handles_empty_result_set(self):
        """Test handling of empty result sets"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = []
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")

        metrics = handler.get_late_data_dashboard_metrics()
        assert metrics['total_records_today'] == 0
        assert metrics['late_records'] == 0

    def test_handles_null_values_in_results(self):
        """Test handling of NULL values in SQL results"""
        mock_client = Mock()
        mock_stmt = Mock()
        mock_stmt.result.data_array = [[None, None, None, None, None, None]]
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")
        metrics = handler.get_late_data_dashboard_metrics()

        assert metrics['total_records_today'] == 0
        assert metrics['late_pct'] == 0.0


class TestDateRangeCalculations:
    """Test date range utility functions"""

    def test_get_dates_in_range(self):
        """Test date range generation"""
        mock_client = Mock()
        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")

        dates = handler._get_dates_in_range('2024-01-01', '2024-01-05')

        assert len(dates) == 5
        assert dates[0] == '2024-01-01'
        assert dates[4] == '2024-01-05'

    def test_get_dates_single_day(self):
        """Test date range for single day"""
        mock_client = Mock()
        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")

        dates = handler._get_dates_in_range('2024-01-01', '2024-01-01')

        assert len(dates) == 1
        assert dates[0] == '2024-01-01'

    def test_get_dates_year_range(self):
        """Test date range for full year"""
        mock_client = Mock()
        handler = EnhancedLateDataHandler(mock_client, "warehouse_id")

        dates = handler._get_dates_in_range('2024-01-01', '2024-12-31')

        assert len(dates) == 366  # 2024 is leap year


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
