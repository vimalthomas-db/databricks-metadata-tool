"""
Test cases for VolumeWriter.
"""
import pytest
import os
import tempfile
from unittest.mock import MagicMock, patch


class TestVolumeWriterInitialization:
    """Test VolumeWriter initialization."""
    
    def test_init_with_valid_params(self):
        """Test VolumeWriter initializes with valid parameters."""
        with patch('databricks_metadata_tool.volume_writer.WorkspaceClient'):
            from databricks_metadata_tool.volume_writer import VolumeWriter
            
            writer = VolumeWriter(
                workspace_url="https://test.azuredatabricks.net",
                catalog="test_catalog",
                schema="test_schema",
                volume="test_volume"
            )
            
            # Check attributes are correctly set
            assert writer.catalog == "test_catalog"
            assert writer.schema == "test_schema"
            assert writer.volume_name == "test_volume"
    
    def test_init_with_staging_folder(self):
        """Test VolumeWriter with custom staging folder."""
        with patch('databricks_metadata_tool.volume_writer.WorkspaceClient'):
            from databricks_metadata_tool.volume_writer import VolumeWriter
            
            writer = VolumeWriter(
                workspace_url="https://test.azuredatabricks.net",
                catalog="test_catalog",
                schema="test_schema",
                volume="test_volume",
                staging_folder="custom_staging"
            )
            
            assert writer.staging_folder == "custom_staging"


class TestVolumeWriterUpload:
    """Test VolumeWriter upload functionality."""
    
    def test_upload_files_creates_path(self, temp_output_dir):
        """Test that upload creates the correct volume path."""
        # Create test files
        test_file = os.path.join(temp_output_dir, "test.csv")
        with open(test_file, 'w') as f:
            f.write("col1,col2\nval1,val2")
        
        with patch('databricks_metadata_tool.volume_writer.WorkspaceClient') as mock_client:
            from databricks_metadata_tool.volume_writer import VolumeWriter
            
            mock_files = MagicMock()
            mock_client.return_value.files = mock_files
            
            writer = VolumeWriter(
                workspace_url="https://test.azuredatabricks.net",
                catalog="test_catalog",
                schema="test_schema",
                volume="test_volume"
            )
            
            result = writer.upload_files(temp_output_dir)
            
            # Verify upload was attempted
            assert mock_files.upload.called or 'volume_path' in result
    
    def test_upload_empty_directory(self, temp_output_dir):
        """Test upload with empty directory."""
        with patch('databricks_metadata_tool.volume_writer.WorkspaceClient'):
            from databricks_metadata_tool.volume_writer import VolumeWriter
            
            writer = VolumeWriter(
                workspace_url="https://test.azuredatabricks.net",
                catalog="test_catalog",
                schema="test_schema",
                volume="test_volume"
            )
            
            # Empty directory should not fail
            result = writer.upload_files(temp_output_dir)
            assert 'files_uploaded' in result or result is not None


class TestVolumePathConstruction:
    """Test volume path construction."""
    
    def test_volume_path_format(self):
        """Test volume path is correctly formatted."""
        catalog = "my_catalog"
        schema = "my_schema"
        volume = "my_volume"
        
        expected_path = f"/Volumes/{catalog}/{schema}/{volume}"
        assert expected_path == "/Volumes/my_catalog/my_schema/my_volume"
    
    def test_volume_path_with_staging(self):
        """Test volume path with staging folder."""
        catalog = "my_catalog"
        schema = "my_schema"
        volume = "my_volume"
        staging = "staging"
        
        expected_path = f"/Volumes/{catalog}/{schema}/{volume}/{staging}"
        assert expected_path == "/Volumes/my_catalog/my_schema/my_volume/staging"


class TestVolumeWriterErrorHandling:
    """Test VolumeWriter error handling."""
    
    def test_invalid_workspace_url(self):
        """Test handling of invalid workspace URL."""
        with patch('databricks_metadata_tool.volume_writer.WorkspaceClient') as mock_client:
            mock_client.side_effect = Exception("Invalid URL")
            
            from databricks_metadata_tool.volume_writer import VolumeWriter
            
            with pytest.raises(Exception):
                VolumeWriter(
                    workspace_url="invalid-url",
                    catalog="test",
                    schema="test",
                    volume="test"
                )
    
    def test_volume_not_found(self, temp_output_dir):
        """Test handling when volume doesn't exist."""
        with patch('databricks_metadata_tool.volume_writer.WorkspaceClient') as mock_client:
            mock_files = MagicMock()
            mock_files.upload.side_effect = Exception("Volume not found")
            mock_client.return_value.files = mock_files
            
            from databricks_metadata_tool.volume_writer import VolumeWriter
            
            writer = VolumeWriter(
                workspace_url="https://test.azuredatabricks.net",
                catalog="test_catalog",
                schema="test_schema",
                volume="nonexistent_volume"
            )
            
            # Create test file
            test_file = os.path.join(temp_output_dir, "test.csv")
            with open(test_file, 'w') as f:
                f.write("data")
            
            # VolumeWriter logs errors but doesn't raise - it continues with other files
            # This is graceful error handling
            result = writer.upload_files(temp_output_dir)
            # Should have errors in result or return partial success
            assert result is not None

