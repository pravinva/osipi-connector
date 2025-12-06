"""
Tests for Main Connector (Orchestration Layer)

Validates end-to-end connector orchestration
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from src.connector.lakeflow_connector import PILakeflowConnector


class TestPILakeflowConnectorInitialization:
    """Test connector initialization"""

    @pytest.fixture
    def basic_config(self):
        """Basic connector configuration"""
        return {
            'pi_web_api_url': 'http://localhost:8000',
            'pi_auth_type': 'basic',
            'pi_username': 'test_user',
            'pi_password': 'test_pass',
            'catalog': 'main',
            'schema': 'bronze',
            'tags': ['TAG001', 'TAG002']
        }

    @patch('src.connector.lakeflow_connector.SparkSession')
    @patch('src.connector.lakeflow_connector.WorkspaceClient')
    def test_connector_initialization_basic_auth(self, mock_ws_client, mock_spark, basic_config):
        """Test connector initializes with basic auth"""
        mock_spark.builder.getOrCreate.return_value = Mock()

        with patch.object(PILakeflowConnector, '_init_auth'):
            with patch.object(PILakeflowConnector, '_init_extractors'):
                with patch.object(PILakeflowConnector, '_init_writers'):
                    connector = PILakeflowConnector(basic_config)

                    assert connector.config == basic_config
                    assert connector.spark is not None
                    assert connector.workspace_client is not None

    @patch('src.connector.lakeflow_connector.SparkSession')
    @patch('src.connector.lakeflow_connector.WorkspaceClient')
    def test_connector_initialization_kerberos_auth(self, mock_ws_client, mock_spark):
        """Test connector initializes with Kerberos auth"""
        config = {
            'pi_web_api_url': 'http://localhost:8000',
            'pi_auth_type': 'kerberos',
            'catalog': 'main',
            'schema': 'bronze',
            'tags': ['TAG001']
        }

        mock_spark.builder.getOrCreate.return_value = Mock()

        with patch.object(PILakeflowConnector, '_init_auth'):
            with patch.object(PILakeflowConnector, '_init_extractors'):
                with patch.object(PILakeflowConnector, '_init_writers'):
                    connector = PILakeflowConnector(config)

                    assert connector.config['pi_auth_type'] == 'kerberos'

    @patch('src.connector.lakeflow_connector.SparkSession')
    @patch('src.connector.lakeflow_connector.WorkspaceClient')
    def test_connector_initialization_oauth_auth(self, mock_ws_client, mock_spark):
        """Test connector initializes with OAuth"""
        config = {
            'pi_web_api_url': 'http://localhost:8000',
            'pi_auth_type': 'oauth',
            'oauth_token': 'test_token_12345',
            'catalog': 'main',
            'schema': 'bronze',
            'tags': ['TAG001']
        }

        mock_spark.builder.getOrCreate.return_value = Mock()

        with patch.object(PILakeflowConnector, '_init_auth'):
            with patch.object(PILakeflowConnector, '_init_extractors'):
                with patch.object(PILakeflowConnector, '_init_writers'):
                    connector = PILakeflowConnector(config)

                    assert connector.config['pi_auth_type'] == 'oauth'
                    assert connector.config['oauth_token'] == 'test_token_12345'


class TestPILakeflowConnectorAuthentication:
    """Test authentication initialization"""

    @pytest.fixture
    def mock_connector_base(self):
        """Create connector with mocked dependencies"""
        config = {
            'pi_web_api_url': 'http://localhost:8000',
            'pi_auth_type': 'basic',
            'pi_username': 'user',
            'pi_password': 'pass',
            'catalog': 'main',
            'schema': 'bronze',
            'tags': []
        }

        with patch('src.connector.lakeflow_connector.SparkSession'):
            with patch('src.connector.lakeflow_connector.WorkspaceClient'):
                with patch.object(PILakeflowConnector, '_init_extractors'):
                    with patch.object(PILakeflowConnector, '_init_writers'):
                        with patch.object(PILakeflowConnector, '_test_connection'):
                            connector = PILakeflowConnector(config)
                            return connector

    def test_basic_authentication_initialization(self, mock_connector_base):
        """Test basic auth is initialized correctly"""
        from requests.auth import HTTPBasicAuth

        assert isinstance(mock_connector_base.auth, HTTPBasicAuth)
        assert mock_connector_base.session is not None


class TestPILakeflowConnectorExtraction:
    """Test data extraction methods"""

    @pytest.fixture
    def mock_connector(self):
        """Create fully mocked connector"""
        config = {
            'pi_web_api_url': 'http://localhost:8000',
            'pi_auth_type': 'basic',
            'pi_username': 'user',
            'pi_password': 'pass',
            'catalog': 'main',
            'schema': 'bronze',
            'tags': ['TAG001', 'TAG002'],
            'af_database_id': 'DB001',
            'include_event_frames': True
        }

        with patch('src.connector.lakeflow_connector.SparkSession'):
            with patch('src.connector.lakeflow_connector.WorkspaceClient'):
                with patch.object(PILakeflowConnector, '_init_auth'):
                    with patch.object(PILakeflowConnector, '_init_extractors'):
                        with patch.object(PILakeflowConnector, '_init_writers'):
                            connector = PILakeflowConnector(config)

                            # Mock extractors
                            connector.ts_extractor = Mock()
                            connector.af_extractor = Mock()
                            connector.ef_extractor = Mock()
                            connector.writer = Mock()
                            connector.session = Mock()

                            return connector

    def test_extract_timeseries(self, mock_connector):
        """Test time-series extraction"""
        # Mock batch response
        mock_connector.session.post.return_value.json.return_value = {
            'Responses': [
                {
                    'Status': 200,
                    'Content': {
                        'Items': [
                            {
                                'Timestamp': '2024-12-06T10:00:00Z',
                                'Value': 75.5,
                                'Good': True,
                                'UnitsAbbreviation': 'degC'
                            }
                        ]
                    }
                }
            ]
        }

        mock_connector.spark.createDataFrame = Mock()

        start_time = datetime(2024, 12, 6, 9, 0, 0)
        end_time = datetime(2024, 12, 6, 10, 0, 0)

        df = mock_connector.extract_timeseries(['TAG001'], start_time, end_time)

        # Verify batch request was made
        mock_connector.session.post.assert_called_once()

    def test_extract_af_hierarchy(self, mock_connector):
        """Test AF hierarchy extraction"""
        mock_df = Mock()
        mock_connector.af_extractor.extract_hierarchy.return_value = mock_df

        result = mock_connector.extract_af_hierarchy('DB001')

        assert result == mock_df
        mock_connector.af_extractor.extract_hierarchy.assert_called_once_with('DB001')

    def test_extract_event_frames(self, mock_connector):
        """Test event frame extraction"""
        mock_df = Mock()
        mock_connector.ef_extractor.extract_event_frames.return_value = mock_df

        start_time = datetime(2024, 12, 6, 9, 0, 0)
        end_time = datetime(2024, 12, 6, 10, 0, 0)

        result = mock_connector.extract_event_frames('DB001', start_time, end_time)

        assert result == mock_df
        mock_connector.ef_extractor.extract_event_frames.assert_called_once()


class TestPILakeflowConnectorOrchestration:
    """Test main orchestration flow"""

    @pytest.fixture
    def mock_connector_full(self):
        """Create connector with all mocks for orchestration test"""
        config = {
            'pi_web_api_url': 'http://localhost:8000',
            'pi_auth_type': 'basic',
            'pi_username': 'user',
            'pi_password': 'pass',
            'catalog': 'main',
            'schema': 'bronze',
            'tags': ['TAG001', 'TAG002'],
            'af_database_id': 'DB001',
            'include_event_frames': True
        }

        with patch('src.connector.lakeflow_connector.SparkSession'):
            with patch('src.connector.lakeflow_connector.WorkspaceClient'):
                with patch.object(PILakeflowConnector, '_init_auth'):
                    with patch.object(PILakeflowConnector, '_init_extractors'):
                        with patch.object(PILakeflowConnector, '_init_writers'):
                            connector = PILakeflowConnector(config)

                            # Mock all components
                            connector.ts_extractor = Mock()
                            connector.af_extractor = Mock()
                            connector.ef_extractor = Mock()
                            connector.writer = Mock()
                            connector.session = Mock()
                            connector.spark = Mock()

                            return connector

    def test_get_tag_list_explicit(self, mock_connector_full):
        """Test getting explicit tag list"""
        tags = mock_connector_full._get_tag_list()

        assert tags == ['TAG001', 'TAG002']

    def test_get_tag_list_all(self, mock_connector_full):
        """Test getting all tags from server"""
        mock_connector_full.config['tags'] = 'all'

        # Mock PI Web API responses
        mock_connector_full.session.get.side_effect = [
            # First call: get data servers
            Mock(json=lambda: {
                'Items': [{'WebId': 'SERVER001'}]
            }),
            # Second call: get points
            Mock(json=lambda: {
                'Items': [
                    {'WebId': 'TAG001'},
                    {'WebId': 'TAG002'},
                    {'WebId': 'TAG003'}
                ]
            })
        ]

        tags = mock_connector_full._get_tag_list()

        assert len(tags) == 3
        assert 'TAG001' in tags

    def test_get_last_checkpoint_time_exists(self, mock_connector_full):
        """Test getting checkpoint time when it exists"""
        mock_row = Mock()
        mock_row.max_time = datetime(2024, 12, 1, 10, 0, 0)

        mock_result = Mock()
        mock_result.collect.return_value = [mock_row]

        mock_connector_full.spark.sql.return_value = mock_result

        last_time = mock_connector_full._get_last_checkpoint_time()

        assert last_time == datetime(2024, 12, 1, 10, 0, 0)

    def test_get_last_checkpoint_time_not_exists(self, mock_connector_full):
        """Test getting checkpoint time when table doesn't exist"""
        mock_connector_full.spark.sql.side_effect = Exception("Table not found")

        last_time = mock_connector_full._get_last_checkpoint_time()

        # Should return default (30 days ago)
        now = datetime.now()
        days_diff = (now - last_time).days
        assert 29 <= days_diff <= 31

    def test_update_checkpoint(self, mock_connector_full):
        """Test updating checkpoint"""
        checkpoint_time = datetime(2024, 12, 6, 10, 0, 0)

        mock_df = Mock()
        mock_connector_full.spark.createDataFrame.return_value = mock_df
        mock_df.write.mode.return_value.saveAsTable = Mock()

        mock_connector_full._update_checkpoint(checkpoint_time)

        # Verify checkpoint was written
        mock_connector_full.spark.createDataFrame.assert_called_once()

    @patch.object(PILakeflowConnector, '_get_tag_list')
    @patch.object(PILakeflowConnector, '_get_last_checkpoint_time')
    @patch.object(PILakeflowConnector, 'extract_timeseries')
    @patch.object(PILakeflowConnector, '_update_checkpoint')
    def test_run_orchestration_basic(
        self,
        mock_update_checkpoint,
        mock_extract_ts,
        mock_get_checkpoint,
        mock_get_tags,
        mock_connector_full
    ):
        """Test basic run orchestration (time-series only)"""
        # Setup mocks
        mock_get_tags.return_value = ['TAG001', 'TAG002']
        mock_get_checkpoint.return_value = datetime(2024, 12, 1, 0, 0, 0)
        mock_extract_ts.return_value = Mock()

        # Remove AF and Event Frame config
        mock_connector_full.config['af_database_id'] = None
        mock_connector_full.config['include_event_frames'] = False

        # Run connector
        mock_connector_full.run()

        # Verify orchestration
        mock_get_tags.assert_called_once()
        mock_get_checkpoint.assert_called_once()
        mock_extract_ts.assert_called_once()
        mock_connector_full.writer.write_timeseries.assert_called_once()
        mock_update_checkpoint.assert_called_once()

    @patch.object(PILakeflowConnector, '_get_tag_list')
    @patch.object(PILakeflowConnector, '_get_last_checkpoint_time')
    @patch.object(PILakeflowConnector, 'extract_timeseries')
    @patch.object(PILakeflowConnector, 'extract_af_hierarchy')
    @patch.object(PILakeflowConnector, 'extract_event_frames')
    @patch.object(PILakeflowConnector, '_update_checkpoint')
    def test_run_orchestration_full(
        self,
        mock_update_checkpoint,
        mock_extract_ef,
        mock_extract_af,
        mock_extract_ts,
        mock_get_checkpoint,
        mock_get_tags,
        mock_connector_full
    ):
        """Test full run orchestration (all features)"""
        # Setup mocks
        mock_get_tags.return_value = ['TAG001', 'TAG002']
        mock_get_checkpoint.return_value = datetime(2024, 12, 1, 0, 0, 0)
        mock_extract_ts.return_value = Mock()
        mock_extract_af.return_value = Mock()
        mock_extract_ef.return_value = Mock()

        # Run connector
        mock_connector_full.run()

        # Verify all extraction methods were called
        mock_extract_ts.assert_called_once()
        mock_extract_af.assert_called_once()
        mock_extract_ef.assert_called_once()

        # Verify all write methods were called
        mock_connector_full.writer.write_timeseries.assert_called_once()
        mock_connector_full.writer.write_af_hierarchy.assert_called_once()
        mock_connector_full.writer.write_event_frames.assert_called_once()

        # Verify checkpoint was updated
        mock_update_checkpoint.assert_called_once()


class TestMainEntryPoint:
    """Test main() entry point"""

    @patch('src.connector.lakeflow_connector.PILakeflowConnector')
    def test_main_with_dbutils(self, mock_connector_class):
        """Test main entry point with dbutils"""
        # This would require mocking dbutils which is complex
        pytest.skip("Requires Databricks runtime")

    @patch('src.connector.lakeflow_connector.PILakeflowConnector')
    @patch.dict('os.environ', {
        'PI_WEB_API_URL': 'http://localhost:8000',
        'PI_AUTH_TYPE': 'basic',
        'CATALOG': 'main',
        'SCHEMA': 'bronze',
        'TAGS': 'TAG001,TAG002',
        'PI_USERNAME': 'user',
        'PI_PASSWORD': 'pass'
    })
    def test_main_with_env_vars(self, mock_connector_class):
        """Test main entry point with environment variables"""
        from src.connector.lakeflow_connector import main

        mock_instance = Mock()
        mock_connector_class.return_value = mock_instance

        # Run main
        main()

        # Verify connector was initialized and run
        mock_connector_class.assert_called_once()
        mock_instance.run.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
