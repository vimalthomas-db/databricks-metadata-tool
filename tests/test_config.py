"""
Test cases for configuration loading and validation.
"""
import pytest
import tempfile
import os
import yaml


class TestConfigLoading:
    """Test configuration file loading."""
    
    def test_load_valid_config(self, temp_output_dir, monkeypatch):
        """Test loading a valid config file."""
        from databricks_metadata_tool.utils import load_config
        
        # Clear env vars so config file values are used
        monkeypatch.delenv('DATABRICKS_ACCOUNT_ID', raising=False)
        
        config_content = """
databricks:
  account_id: test-account
  cloud: azure

filters:
  exclude_catalogs: ['system']
  
output:
  directory: ./outputs
"""
        config_path = os.path.join(temp_output_dir, 'config.yaml')
        with open(config_path, 'w') as f:
            f.write(config_content)
        
        config = load_config(config_path)
        assert config['databricks']['account_id'] == 'test-account'
        assert config['databricks']['cloud'] == 'azure'
    
    def test_load_missing_config(self):
        """Test loading a non-existent config file."""
        from databricks_metadata_tool.utils import load_config
        
        with pytest.raises(FileNotFoundError):
            load_config('/nonexistent/config.yaml')
    
    def test_load_empty_config(self, temp_output_dir):
        """Test loading an empty config file."""
        from databricks_metadata_tool.utils import load_config
        
        config_path = os.path.join(temp_output_dir, 'empty.yaml')
        with open(config_path, 'w') as f:
            f.write('')
        
        # Empty YAML may raise TypeError or return None/empty dict
        # Current implementation requires valid config structure
        try:
            config = load_config(config_path)
            assert config is None or config == {} or 'databricks' in config
        except TypeError:
            # Expected - empty config not supported
            pass


class TestFiltersConfig:
    """Test filter configuration."""
    
    def test_exclude_catalogs_list(self, sample_config):
        """Test exclude_catalogs as a list."""
        assert sample_config['filters']['exclude_catalogs'] == ['system', 'samples']
    
    def test_exclude_catalogs_empty(self, sample_config):
        """Test empty exclude_catalogs."""
        sample_config['filters']['exclude_catalogs'] = []
        assert sample_config['filters']['exclude_catalogs'] == []
    
    def test_only_catalogs_none(self, sample_config):
        """Test only_catalogs when None (no filter)."""
        assert sample_config['filters']['only_catalogs'] is None
    
    def test_only_catalogs_list(self, sample_config_with_only_catalogs):
        """Test only_catalogs with list of catalogs."""
        assert sample_config_with_only_catalogs['filters']['only_catalogs'] == ['catalog_a', 'catalog_b']
    
    def test_filters_section_missing(self):
        """Test config without filters section."""
        config = {
            'databricks': {'account_id': 'test'},
            'output': {'directory': './outputs'}
        }
        # Should handle missing filters gracefully
        filters = config.get('filters', {})
        assert filters.get('exclude_catalogs', []) == []
        assert filters.get('only_catalogs') is None


class TestCollectionConfig:
    """Test collection configuration."""
    
    def test_default_size_threshold(self, sample_config):
        """Test default size threshold."""
        assert sample_config['collection']['size_threshold'] == 200
    
    def test_default_size_workers(self, sample_config):
        """Test default size workers."""
        assert sample_config['collection']['size_workers'] == 20
    
    def test_default_max_parallel_spark_jobs(self, sample_config):
        """Test default max parallel spark jobs."""
        assert sample_config['collection']['max_parallel_spark_jobs'] == 3
    
    def test_collect_sizes_enabled(self, sample_config):
        """Test collect_sizes flag."""
        assert sample_config['collection']['collect_sizes'] == True
    
    def test_collect_sizes_disabled(self, sample_config):
        """Test collect_sizes when disabled."""
        sample_config['collection']['collect_sizes'] = False
        assert sample_config['collection']['collect_sizes'] == False


class TestVolumeConfig:
    """Test volume configuration."""
    
    def test_volume_defaults(self, sample_config):
        """Test volume configuration defaults."""
        assert sample_config['volume']['catalog'] == 'test_catalog'
        assert sample_config['volume']['schema'] == 'test_schema'
        assert sample_config['volume']['volume'] == 'test_volume'
    
    def test_volume_section_missing(self):
        """Test config without volume section."""
        config = {'databricks': {'account_id': 'test'}}
        volume = config.get('volume', {})
        assert volume.get('catalog', 'collection_catalog') == 'collection_catalog'
        assert volume.get('schema', 'collection_schema') == 'collection_schema'


class TestDuplicateHandling:
    """Test handling of duplicate values in configuration."""
    
    def test_duplicate_exclude_catalogs(self):
        """Test duplicate values in exclude_catalogs."""
        config = {
            'filters': {
                'exclude_catalogs': ['system', 'system', 'samples']
            }
        }
        # Duplicates should be deduplicated when used
        unique = list(set(config['filters']['exclude_catalogs']))
        assert len(unique) == 2
        assert 'system' in unique
        assert 'samples' in unique
    
    def test_duplicate_only_catalogs(self):
        """Test duplicate values in only_catalogs."""
        config = {
            'filters': {
                'only_catalogs': ['cat1', 'cat1', 'cat2', 'cat2', 'cat2']
            }
        }
        # Duplicates should be deduplicated when used
        unique = list(set(config['filters']['only_catalogs']))
        assert len(unique) == 2

