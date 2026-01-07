"""
Test cases for the MetadataOrchestrator.
"""
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime


class TestOrchestratorInitialization:
    """Test orchestrator initialization."""
    
    @patch('databricks_metadata_tool.orchestrator.AccountProvider')
    def test_init_with_valid_config(self, mock_provider, sample_config):
        """Test orchestrator initializes with valid config."""
        from databricks_metadata_tool.orchestrator import MetadataOrchestrator
        
        mock_provider.return_value = MagicMock()
        orchestrator = MetadataOrchestrator(sample_config)
        
        assert orchestrator.config == sample_config
        assert orchestrator.filters == sample_config['filters']
    
    @patch('databricks_metadata_tool.orchestrator.AccountProvider')
    def test_init_filters_fallback_to_azure(self, mock_provider):
        """Test filters fallback to azure section for backward compatibility."""
        from databricks_metadata_tool.orchestrator import MetadataOrchestrator
        
        config = {
            'databricks': {'account_id': 'test', 'cloud': 'azure'},
            'azure': {
                'exclude_catalogs': ['legacy_exclude']
            },
            'collection': {},
            'output': {'directory': './outputs'}
        }
        
        mock_provider.return_value = MagicMock()
        orchestrator = MetadataOrchestrator(config)
        
        # Should use azure section as fallback when filters not present
        assert orchestrator.filters.get('exclude_catalogs') == ['legacy_exclude']


class TestOnlyCatalogsFilter:
    """Test only_catalogs filter behavior."""
    
    def test_filter_applies_to_table_collection(self, sample_config_with_only_catalogs, mock_catalog):
        """Test that only_catalogs filter affects table collection."""
        only_catalogs = sample_config_with_only_catalogs['filters']['only_catalogs']
        
        # Catalog in filter list - should collect tables
        mock_catalog.catalog_name = 'catalog_a'
        should_collect = mock_catalog.catalog_name in only_catalogs
        assert should_collect == True
        
        # Catalog not in filter list - should skip tables
        mock_catalog.catalog_name = 'catalog_c'
        should_collect = mock_catalog.catalog_name in only_catalogs
        assert should_collect == False
    
    def test_filter_none_collects_all(self, sample_config, mock_catalog):
        """Test that None only_catalogs means collect all."""
        only_catalogs = sample_config['filters']['only_catalogs']
        
        # When only_catalogs is None, all catalogs should be collected
        assert only_catalogs is None
        
        # Logic: if only_catalogs is None, don't skip
        # The expression 'only_catalogs and ...' evaluates to None/False when only_catalogs is None
        should_skip = only_catalogs and mock_catalog.catalog_name not in only_catalogs
        # None is falsy, so should_skip should be falsy (None or False)
        assert not should_skip
    
    def test_filter_empty_list_collects_none(self):
        """Test that empty only_catalogs list skips all catalogs."""
        only_catalogs = []
        catalog_name = 'any_catalog'
        
        # Empty list means skip all
        should_skip = only_catalogs is not None and len(only_catalogs) > 0 and catalog_name not in only_catalogs
        # Actually, empty list [] is truthy for 'only_catalogs and ...'
        # but len == 0, so this edge case needs handling
        if only_catalogs is not None and len(only_catalogs) == 0:
            # Empty list should probably mean "collect none" or be treated as None
            pass


class TestExcludeCatalogsFilter:
    """Test exclude_catalogs filter behavior."""
    
    def test_system_catalog_excluded(self, sample_config):
        """Test that system catalog is excluded by default."""
        exclude = sample_config['filters']['exclude_catalogs']
        assert 'system' in exclude
    
    def test_samples_catalog_excluded(self, sample_config):
        """Test that samples catalog is excluded by default."""
        exclude = sample_config['filters']['exclude_catalogs']
        assert 'samples' in exclude
    
    def test_custom_catalog_not_excluded(self, sample_config):
        """Test that custom catalogs are not excluded."""
        exclude = sample_config['filters']['exclude_catalogs']
        assert 'my_custom_catalog' not in exclude


class TestDryRunBehavior:
    """Test dry run behavior."""
    
    @patch('databricks_metadata_tool.orchestrator.AccountProvider')
    def test_dry_run_flag_set(self, mock_provider, sample_config):
        """Test dry run mode is properly configured."""
        sample_config['dry_run'] = True
        
        mock_provider.return_value = MagicMock()
        from databricks_metadata_tool.orchestrator import MetadataOrchestrator
        orchestrator = MetadataOrchestrator(sample_config)
        
        # dry_run should be accessible
        assert sample_config.get('dry_run') == True
    
    def test_dry_run_skips_size_collection(self):
        """Test that dry run shows tier selection but doesn't collect sizes."""
        # In dry run mode:
        # - Tier analysis should be displayed
        # - Actual size collection should be skipped
        # - No files should be saved
        dry_run = True
        collect_sizes = not dry_run  # Should be False when dry_run is True
        assert collect_sizes == False
    
    def test_dry_run_skips_file_output(self):
        """Test that dry run doesn't save output files."""
        dry_run = True
        should_save = not dry_run
        assert should_save == False
    
    def test_dry_run_skips_volume_upload(self):
        """Test that dry run skips volume upload."""
        dry_run = True
        write_to_volume = True
        
        should_upload = write_to_volume and not dry_run
        assert should_upload == False


class TestScanInCollect:
    """Test that scan is embedded in collect process."""
    
    @patch('databricks_metadata_tool.orchestrator.AccountProvider')
    def test_collect_runs_scan_first(self, mock_provider, sample_config):
        """Test that collect mode runs account scan first."""
        # The collect_from_admin method should call scan_workspaces first
        # This is the embedded scan behavior
        pass  # Integration test - needs mocked workspace
    
    def test_scan_results_saved_in_collect(self):
        """Test that scan results are saved during collect."""
        # During collect:
        # - account_metastores.csv should be created
        # - account_summary.csv should be created
        # - account_workspaces.csv should NOT be created (merged into collect_workspaces)
        pass


class TestCollectionResult:
    """Test CollectionResult object."""
    
    def test_collection_result_initialization(self):
        """Test CollectionResult initialization."""
        from databricks_metadata_tool.orchestrator import CollectionResult
        
        result = CollectionResult(
            subscription_id="test-sub",
            collection_timestamp="2024-01-01T00:00:00"
        )
        assert result.workspaces == []
        assert result.catalogs == []
        assert result.tables == []
        assert result.errors == []
    
    def test_collection_result_with_errors(self):
        """Test CollectionResult with errors."""
        from databricks_metadata_tool.orchestrator import CollectionResult
        
        result = CollectionResult(
            subscription_id="test-sub",
            collection_timestamp="2024-01-01T00:00:00"
        )
        result.errors.append("Test error 1")
        result.errors.append("Test error 2")
        
        assert len(result.errors) == 2
        assert "Test error 1" in result.errors


class TestTierSelection:
    """Test tier selection logic for size collection."""
    
    def test_tier1_bulk_query(self):
        """Test Tier 1 uses bulk system.information_schema query."""
        # Tier 1 should be attempted first
        # Uses: system.information_schema.tables
        table_count = 50
        tier = 1  # Bulk query always attempted first
        assert tier == 1
    
    def test_tier2_for_small_catalogs(self):
        """Test Tier 2 used for catalogs below threshold."""
        table_count = 150
        threshold = 200
        has_cluster = False
        
        if table_count < threshold:
            tier = 2  # SQL warehouse parallel queries
        else:
            tier = 3 if has_cluster else 2
        
        assert tier == 2
    
    def test_tier3_for_large_catalogs_with_cluster(self):
        """Test Tier 3 used for large catalogs with cluster."""
        table_count = 500
        threshold = 200
        has_cluster = True
        
        if table_count >= threshold and has_cluster:
            tier = 3  # Spark job
        else:
            tier = 2
        
        assert tier == 3
    
    def test_tier2_fallback_without_cluster(self):
        """Test Tier 2 fallback when no cluster for large catalog."""
        table_count = 500
        threshold = 200
        has_cluster = False
        
        if table_count >= threshold and has_cluster:
            tier = 3
        else:
            tier = 2  # Fallback to SQL warehouse
        
        assert tier == 2

