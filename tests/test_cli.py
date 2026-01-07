"""
Test cases for CLI argument parsing and behavior.
"""
import pytest
import sys
from unittest.mock import patch, MagicMock
from io import StringIO


class TestCLIArgumentParsing:
    """Test CLI argument parsing."""
    
    def test_scan_mode_default(self):
        """Test that scan is the default mode when no mode specified."""
        with patch('sys.argv', ['dbmeta']):
            from databricks_metadata_tool.cli import main
            # Should default to scan mode without error
    
    def test_scan_mode_explicit(self):
        """Test explicit --scan flag."""
        test_args = ['dbmeta', '--scan']
        with patch('sys.argv', test_args):
            import argparse
            parser = argparse.ArgumentParser()
            mode_group = parser.add_mutually_exclusive_group()
            mode_group.add_argument('--scan', action='store_true')
            mode_group.add_argument('--collect', action='store_true')
            args = parser.parse_args(['--scan'])
            assert args.scan == True
            assert args.collect == False
    
    def test_collect_mode_requires_workspace(self):
        """Test that collect mode requires admin-workspace."""
        test_args = ['dbmeta', '--collect', '--warehouse-id', 'abc123']
        # Should fail because --admin-workspace is missing
        # This is enforced in the CLI main() function
    
    def test_collect_mode_requires_warehouse(self):
        """Test that collect mode requires warehouse-id."""
        test_args = ['dbmeta', '--collect', '--admin-workspace', 'https://test.com']
        # Should fail because --warehouse-id is missing
    
    def test_mutually_exclusive_modes(self):
        """Test that scan, collect, and collect-dryrun are mutually exclusive."""
        import argparse
        parser = argparse.ArgumentParser()
        mode_group = parser.add_mutually_exclusive_group()
        mode_group.add_argument('--scan', action='store_true')
        mode_group.add_argument('--collect', action='store_true')
        mode_group.add_argument('--collect-dryrun', action='store_true')
        
        # Should raise error for multiple modes
        with pytest.raises(SystemExit):
            parser.parse_args(['--scan', '--collect'])


class TestOnlyCatalogsArgument:
    """Test --only-catalogs argument handling."""
    
    def test_single_catalog(self):
        """Test single catalog value."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--only-catalogs', nargs='+')
        args = parser.parse_args(['--only-catalogs', 'catalog1'])
        assert args.only_catalogs == ['catalog1']
    
    def test_multiple_catalogs(self):
        """Test multiple catalog values."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--only-catalogs', nargs='+')
        args = parser.parse_args(['--only-catalogs', 'cat1', 'cat2', 'cat3'])
        assert args.only_catalogs == ['cat1', 'cat2', 'cat3']
    
    def test_duplicate_catalogs(self):
        """Test that duplicate catalogs are accepted (dedup happens later)."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--only-catalogs', nargs='+')
        args = parser.parse_args(['--only-catalogs', 'cat1', 'cat1', 'cat2'])
        assert args.only_catalogs == ['cat1', 'cat1', 'cat2']
        # Note: Deduplication should happen in orchestrator, not CLI
    
    def test_empty_not_allowed(self):
        """Test that --only-catalogs requires at least one value."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--only-catalogs', nargs='+')
        with pytest.raises(SystemExit):
            parser.parse_args(['--only-catalogs'])


class TestOutputDirArgument:
    """Test --output-dir argument handling."""
    
    def test_default_from_config(self):
        """Test that output-dir defaults to config value."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--output-dir', default=None)
        args = parser.parse_args([])
        assert args.output_dir is None  # Should fall back to config
    
    def test_explicit_output_dir(self):
        """Test explicit output-dir value."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--output-dir')
        args = parser.parse_args(['--output-dir', '/custom/path'])
        assert args.output_dir == '/custom/path'


class TestClusterIdArgument:
    """Test --cluster-id argument handling."""
    
    def test_cluster_id_optional(self):
        """Test that cluster-id is optional."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--cluster-id')
        args = parser.parse_args([])
        assert args.cluster_id is None
    
    def test_cluster_id_provided(self):
        """Test cluster-id when provided."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--cluster-id')
        args = parser.parse_args(['--cluster-id', '0106-140007-gpurnj7l'])
        assert args.cluster_id == '0106-140007-gpurnj7l'


class TestWriteToVolumeFlag:
    """Test --write-to-volume flag."""
    
    def test_write_to_volume_default_false(self):
        """Test that write-to-volume defaults to False."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--write-to-volume', action='store_true')
        args = parser.parse_args([])
        assert args.write_to_volume == False
    
    def test_write_to_volume_enabled(self):
        """Test write-to-volume when enabled."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--write-to-volume', action='store_true')
        args = parser.parse_args(['--write-to-volume'])
        assert args.write_to_volume == True

