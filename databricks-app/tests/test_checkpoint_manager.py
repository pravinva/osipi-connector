"""
Tests for Checkpoint Manager

Validates incremental ingestion state management
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from src.checkpoints.checkpoint_manager import CheckpointManager


class TestCheckpointManager:
    """Test checkpoint manager functionality"""

    @pytest.fixture
    def mock_spark(self):
        """Mock Spark session"""
        spark = Mock()
        spark.sql = Mock()
        spark.table = Mock()
        spark.createDataFrame = Mock()
        return spark

    @pytest.fixture
    def mock_workspace_client(self):
        """Mock Databricks WorkspaceClient"""
        client = Mock()
        client.tables = Mock()
        return client

    @pytest.fixture
    def checkpoint_manager(self, mock_spark, mock_workspace_client):
        """Create checkpoint manager instance"""
        with patch.object(CheckpointManager, '_ensure_checkpoint_table_exists'):
            manager = CheckpointManager(
                spark=mock_spark,
                checkpoint_table="test_catalog.checkpoints.pi_watermarks",
                workspace_client=mock_workspace_client
            )
        return manager

    def test_checkpoint_manager_initialization(self, mock_spark, mock_workspace_client):
        """Test checkpoint manager initializes correctly"""
        manager = CheckpointManager(
            spark=mock_spark,
            checkpoint_table="test_catalog.checkpoints.pi_watermarks",
            workspace_client=mock_workspace_client
        )

        assert manager.spark == mock_spark
        assert manager.workspace_client == mock_workspace_client
        assert manager.checkpoint_table == "test_catalog.checkpoints.pi_watermarks"

    def test_get_watermarks_with_existing_checkpoints(self, checkpoint_manager, mock_spark):
        """Test getting watermarks for tags with existing checkpoints"""
        # Mock checkpoint table data
        mock_row_1 = Mock()
        mock_row_1.tag_webid = "TAG001"
        mock_row_1.last_timestamp = datetime(2024, 12, 1, 10, 0, 0)

        mock_row_2 = Mock()
        mock_row_2.tag_webid = "TAG002"
        mock_row_2.last_timestamp = datetime(2024, 12, 1, 11, 0, 0)

        mock_df = Mock()
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.collect.return_value = [mock_row_1, mock_row_2]

        mock_spark.table.return_value = mock_df

        # Get watermarks
        tag_webids = ["TAG001", "TAG002"]
        watermarks = checkpoint_manager.get_watermarks(tag_webids)

        # Verify
        assert len(watermarks) == 2
        assert watermarks["TAG001"] == datetime(2024, 12, 1, 10, 0, 0)
        assert watermarks["TAG002"] == datetime(2024, 12, 1, 11, 0, 0)

    def test_get_watermarks_with_new_tags(self, checkpoint_manager, mock_spark):
        """Test getting watermarks for new tags (no checkpoint)"""
        # Mock empty checkpoint table
        mock_df = Mock()
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.collect.return_value = []

        mock_spark.table.return_value = mock_df

        # Get watermarks
        tag_webids = ["NEW_TAG001", "NEW_TAG002"]
        watermarks = checkpoint_manager.get_watermarks(tag_webids)

        # Verify - should use default (30 days ago)
        assert len(watermarks) == 2
        assert "NEW_TAG001" in watermarks
        assert "NEW_TAG002" in watermarks

        # Check default is approximately 30 days ago
        now = datetime.now()
        for tag, timestamp in watermarks.items():
            days_diff = (now - timestamp).days
            assert 29 <= days_diff <= 31  # Allow some time for test execution

    def test_get_watermarks_mixed_tags(self, checkpoint_manager, mock_spark):
        """Test getting watermarks for mix of existing and new tags"""
        # Mock checkpoint table with one existing tag
        mock_row = Mock()
        mock_row.tag_webid = "EXISTING_TAG"
        mock_row.last_timestamp = datetime(2024, 12, 1, 10, 0, 0)

        mock_df = Mock()
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.collect.return_value = [mock_row]

        mock_spark.table.return_value = mock_df

        # Get watermarks for existing and new tags
        tag_webids = ["EXISTING_TAG", "NEW_TAG"]
        watermarks = checkpoint_manager.get_watermarks(tag_webids)

        # Verify
        assert len(watermarks) == 2
        assert watermarks["EXISTING_TAG"] == datetime(2024, 12, 1, 10, 0, 0)
        assert isinstance(watermarks["NEW_TAG"], datetime)

    def test_get_watermarks_empty_list(self, checkpoint_manager):
        """Test getting watermarks with empty tag list"""
        watermarks = checkpoint_manager.get_watermarks([])
        assert watermarks == {}

    def test_update_watermarks(self, checkpoint_manager, mock_spark):
        """Test updating checkpoints after successful ingestion"""
        # Mock DataFrame creation
        mock_df = Mock()
        mock_spark.createDataFrame.return_value = mock_df
        mock_df.createOrReplaceTempView = Mock()

        # Mock SQL execution
        mock_spark.sql.return_value = None

        # Update watermarks
        tag_data = {
            "TAG001": {
                "tag_name": "Plant1.Temperature",
                "max_timestamp": datetime(2024, 12, 6, 10, 0, 0),
                "record_count": 1000
            },
            "TAG002": {
                "tag_name": "Plant1.Pressure",
                "max_timestamp": datetime(2024, 12, 6, 10, 5, 0),
                "record_count": 500
            }
        }

        checkpoint_manager.update_watermarks(tag_data)

        # Verify DataFrame was created with correct data
        mock_spark.createDataFrame.assert_called_once()
        call_args = mock_spark.createDataFrame.call_args[0][0]
        assert len(call_args) == 2

        # Verify temp view was created
        mock_df.createOrReplaceTempView.assert_called_once_with("checkpoint_updates")

        # Verify MERGE statement was executed
        mock_spark.sql.assert_called_once()
        merge_sql = mock_spark.sql.call_args[0][0]
        assert "MERGE INTO" in merge_sql
        assert "checkpoint_updates" in merge_sql

    def test_get_checkpoint_table_info_success(self, checkpoint_manager, mock_workspace_client):
        """Test getting checkpoint table info using SDK"""
        mock_table_info = Mock()
        mock_table_info.name = "pi_watermarks"
        mock_workspace_client.tables.get.return_value = mock_table_info

        table_info = checkpoint_manager.get_checkpoint_table_info()

        assert table_info == mock_table_info
        mock_workspace_client.tables.get.assert_called_once_with(
            "test_catalog.checkpoints.pi_watermarks"
        )

    def test_get_checkpoint_table_info_not_found(self, checkpoint_manager, mock_workspace_client):
        """Test getting checkpoint table info when table doesn't exist"""
        mock_workspace_client.tables.get.side_effect = Exception("Table not found")

        table_info = checkpoint_manager.get_checkpoint_table_info()

        assert table_info is None

    def test_get_checkpoint_stats(self, checkpoint_manager, mock_spark):
        """Test getting checkpoint statistics"""
        # Mock stats query result
        mock_row = Mock()
        mock_row.total_tags = 100
        mock_row.oldest_checkpoint = datetime(2024, 11, 1, 0, 0, 0)
        mock_row.newest_checkpoint = datetime(2024, 12, 6, 10, 0, 0)
        mock_row.oldest_run = datetime(2024, 11, 1, 1, 0, 0)
        mock_row.newest_run = datetime(2024, 12, 6, 10, 30, 0)
        mock_row.total_records = 1000000

        mock_result = Mock()
        mock_result.collect.return_value = [mock_row]
        mock_spark.sql.return_value = mock_result

        stats = checkpoint_manager.get_checkpoint_stats()

        # Verify
        assert stats['total_tags'] == 100
        assert stats['oldest_checkpoint'] == datetime(2024, 11, 1, 0, 0, 0)
        assert stats['newest_checkpoint'] == datetime(2024, 12, 6, 10, 0, 0)
        assert stats['total_records'] == 1000000

    def test_get_checkpoint_stats_error(self, checkpoint_manager, mock_spark):
        """Test getting checkpoint stats with error"""
        mock_spark.sql.side_effect = Exception("Query failed")

        stats = checkpoint_manager.get_checkpoint_stats()

        assert stats == {}

    def test_reset_checkpoint_single_tag(self, checkpoint_manager, mock_spark):
        """Test resetting checkpoint for single tag"""
        checkpoint_manager.reset_checkpoint("TAG001")

        # Verify DELETE statement was executed
        mock_spark.sql.assert_called_once()
        delete_sql = mock_spark.sql.call_args[0][0]
        assert "DELETE FROM" in delete_sql
        assert "TAG001" in delete_sql

    def test_reset_all_checkpoints(self, checkpoint_manager, mock_spark):
        """Test resetting all checkpoints"""
        checkpoint_manager.reset_all_checkpoints()

        # Verify DELETE statement was executed
        mock_spark.sql.assert_called_once()
        delete_sql = mock_spark.sql.call_args[0][0]
        assert "DELETE FROM" in delete_sql
        assert "test_catalog.checkpoints.pi_watermarks" in delete_sql


class TestCheckpointManagerIntegration:
    """Integration tests with mock data"""

    def test_checkpoint_workflow(self):
        """Test complete checkpoint workflow"""
        # This would require a real Spark session
        # For now, we'll skip integration tests
        pytest.skip("Requires Spark session")

    def test_incremental_ingestion_pattern(self):
        """Test incremental ingestion pattern"""
        pytest.skip("Requires Spark session")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
