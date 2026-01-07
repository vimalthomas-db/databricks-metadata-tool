"""
Integration tests for end-to-end workflows.

These tests require actual Databricks connectivity and should be run
with proper environment variables set.

Run with: pytest tests/test_integration.py -v --run-integration
"""
import pytest
import os


# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


def pytest_configure(config):
    """Add integration marker."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )


class TestScanWorkflow:
    """Test scan workflow end-to-end."""
    
    @pytest.mark.skipif(
        not os.getenv('DATABRICKS_ACCOUNT_ID'),
        reason="DATABRICKS_ACCOUNT_ID not set"
    )
    def test_scan_discovers_workspaces(self, temp_output_dir, sample_config):
        """Test that scan discovers workspaces."""
        from databricks_metadata_tool.orchestrator import MetadataOrchestrator
        
        sample_config['output']['directory'] = temp_output_dir
        orchestrator = MetadataOrchestrator(sample_config)
        
        result = orchestrator.scan_workspaces(discovery_mode='account')
        
        assert result is not None
        # Should have at least discovered something or returned empty result
    
    @pytest.mark.skipif(
        not os.getenv('DATABRICKS_ACCOUNT_ID'),
        reason="DATABRICKS_ACCOUNT_ID not set"
    )
    def test_scan_creates_output_files(self, temp_output_dir, sample_config):
        """Test that scan creates expected output files."""
        from databricks_metadata_tool.orchestrator import MetadataOrchestrator
        
        sample_config['output']['directory'] = temp_output_dir
        orchestrator = MetadataOrchestrator(sample_config)
        
        orchestrator.scan_workspaces(discovery_mode='account')
        
        # Check expected files exist
        files = os.listdir(temp_output_dir)
        assert any('account_metastores' in f for f in files) or len(files) > 0


class TestCollectWorkflow:
    """Test collect workflow end-to-end."""
    
    @pytest.mark.skipif(
        not all([
            os.getenv('DATABRICKS_ACCOUNT_ID'),
            os.getenv('TEST_ADMIN_WORKSPACE'),
            os.getenv('TEST_WAREHOUSE_ID')
        ]),
        reason="Required environment variables not set"
    )
    def test_collect_with_only_catalogs(self, temp_output_dir, sample_config):
        """Test collect with only_catalogs filter."""
        from databricks_metadata_tool.orchestrator import MetadataOrchestrator
        
        sample_config['output']['directory'] = temp_output_dir
        orchestrator = MetadataOrchestrator(sample_config)
        
        result = orchestrator.collect_from_admin(
            admin_workspace=os.getenv('TEST_ADMIN_WORKSPACE'),
            warehouse_id=os.getenv('TEST_WAREHOUSE_ID'),
            only_catalogs=['nonexistent_catalog']  # Filter to non-existent
        )
        
        # Should complete without errors (just no tables)
        assert result is not None


class TestDryRunWorkflow:
    """Test dry run workflow end-to-end."""
    
    @pytest.mark.skipif(
        not all([
            os.getenv('DATABRICKS_ACCOUNT_ID'),
            os.getenv('TEST_ADMIN_WORKSPACE'),
            os.getenv('TEST_WAREHOUSE_ID')
        ]),
        reason="Required environment variables not set"
    )
    def test_dry_run_no_files_created(self, temp_output_dir, sample_config):
        """Test that dry run doesn't create output files."""
        from databricks_metadata_tool.orchestrator import MetadataOrchestrator
        
        sample_config['output']['directory'] = temp_output_dir
        sample_config['dry_run'] = True
        orchestrator = MetadataOrchestrator(sample_config)
        
        result = orchestrator.collect_from_admin(
            admin_workspace=os.getenv('TEST_ADMIN_WORKSPACE'),
            warehouse_id=os.getenv('TEST_WAREHOUSE_ID'),
            dry_run=True
        )
        
        # Dry run should not create table files
        files = os.listdir(temp_output_dir)
        table_files = [f for f in files if 'tables' in f.lower()]
        assert len(table_files) == 0


class TestVolumeUploadWorkflow:
    """Test volume upload workflow end-to-end."""
    
    @pytest.mark.skipif(
        not all([
            os.getenv('DATABRICKS_ACCOUNT_ID'),
            os.getenv('TEST_ADMIN_WORKSPACE'),
            os.getenv('TEST_VOLUME_CATALOG'),
            os.getenv('TEST_VOLUME_SCHEMA'),
            os.getenv('TEST_VOLUME_NAME')
        ]),
        reason="Required environment variables not set"
    )
    def test_volume_upload_success(self, temp_output_dir):
        """Test successful volume upload."""
        from databricks_metadata_tool.volume_writer import VolumeWriter
        
        # Create test file
        test_file = os.path.join(temp_output_dir, "test_upload.csv")
        with open(test_file, 'w') as f:
            f.write("col1,col2\nval1,val2")
        
        writer = VolumeWriter(
            workspace_url=os.getenv('TEST_ADMIN_WORKSPACE'),
            catalog=os.getenv('TEST_VOLUME_CATALOG'),
            schema=os.getenv('TEST_VOLUME_SCHEMA'),
            volume=os.getenv('TEST_VOLUME_NAME')
        )
        
        result = writer.upload_files(temp_output_dir)
        
        assert result is not None
        assert result.get('files_uploaded', 0) > 0


class TestSparkJobWorkflow:
    """Test Spark job submission workflow."""
    
    @pytest.mark.skipif(
        not all([
            os.getenv('DATABRICKS_ACCOUNT_ID'),
            os.getenv('TEST_ADMIN_WORKSPACE'),
            os.getenv('TEST_WAREHOUSE_ID'),
            os.getenv('TEST_CLUSTER_ID')
        ]),
        reason="Required environment variables not set"
    )
    def test_spark_job_for_large_catalog(self, temp_output_dir, sample_config):
        """Test Spark job submission for large catalogs."""
        from databricks_metadata_tool.orchestrator import MetadataOrchestrator
        
        sample_config['output']['directory'] = temp_output_dir
        sample_config['collection']['size_threshold'] = 10  # Low threshold for testing
        
        orchestrator = MetadataOrchestrator(sample_config)
        
        result = orchestrator.collect_from_admin(
            admin_workspace=os.getenv('TEST_ADMIN_WORKSPACE'),
            warehouse_id=os.getenv('TEST_WAREHOUSE_ID'),
            cluster_id=os.getenv('TEST_CLUSTER_ID'),
            size_threshold=10  # Low threshold to trigger Spark
        )
        
        assert result is not None

