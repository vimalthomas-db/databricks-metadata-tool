"""Upload collection output files to a Unity Catalog volume."""

import logging
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType

logger = logging.getLogger('databricks_metadata_tool.volume_writer')

# Default names for collection output (can be overridden via config.yaml)
DEFAULT_CATALOG = "collection_catalog"
DEFAULT_SCHEMA = "collection_schema"
DEFAULT_VOLUME = "collection_volume"
DEFAULT_STAGING_FOLDER = "staging"


class VolumeWriter:
    """Upload CSV files to a Unity Catalog volume."""
    
    def __init__(self, workspace_url: str, catalog: str = None, schema: str = None, 
                 volume: str = None, staging_folder: str = None):
        self.workspace_url = workspace_url
        self.catalog = catalog or DEFAULT_CATALOG
        self.schema = schema or DEFAULT_SCHEMA
        self.volume_name = volume or DEFAULT_VOLUME
        self.staging_folder = staging_folder or DEFAULT_STAGING_FOLDER
        
        self.client = WorkspaceClient(host=workspace_url)
    
    def ensure_catalog_exists(self):
        """Create catalog if it doesn't exist."""
        try:
            self.client.catalogs.get(self.catalog)
            logger.info(f"Catalog exists: {self.catalog}")
        except Exception:
            logger.info(f"Creating catalog: {self.catalog}")
            self.client.catalogs.create(
                name=self.catalog,
                comment="Databricks metadata collection outputs"
            )
            logger.info(f"Catalog created: {self.catalog}")
    
    def ensure_schema_exists(self):
        """Create schema if it doesn't exist."""
        schema_full_name = f"{self.catalog}.{self.schema}"
        try:
            self.client.schemas.get(schema_full_name)
            logger.info(f"Schema exists: {schema_full_name}")
        except Exception:
            logger.info(f"Creating schema: {schema_full_name}")
            self.client.schemas.create(
                catalog_name=self.catalog,
                name=self.schema,
                comment="Collection output schema"
            )
            logger.info(f"Schema created: {schema_full_name}")
        
    def ensure_volume_exists(self) -> str:
        """Create the full hierarchy: catalog, schema, volume."""
        self.ensure_catalog_exists()
        self.ensure_schema_exists()
        
        volume_full_name = f"{self.catalog}.{self.schema}.{self.volume_name}"
        
        try:
            self.client.volumes.read(volume_full_name)
            logger.info(f"Volume exists: {volume_full_name}")
        except Exception:
            logger.info(f"Creating volume: {volume_full_name}")
            self.client.volumes.create(
                catalog_name=self.catalog,
                schema_name=self.schema,
                name=self.volume_name,
                volume_type=VolumeType.MANAGED,
                comment="Metadata collection output files"
            )
            logger.info(f"Volume created: {volume_full_name}")
        
        return volume_full_name
    
    def get_volume_path(self) -> str:
        """Get the full volume path for uploads."""
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume_name}/{self.staging_folder}"
    
    def upload_file(self, file_path: str) -> bool:
        """
        Upload a single file to the volume.
        
        Args:
            file_path: Local path to the file to upload
            
        Returns:
            True if successful, False otherwise
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                logger.warning(f"File not found: {file_path}")
                return False
            
            volume_path = self.get_volume_path()
            remote_path = f"{volume_path}/{file_path.name}"
            
            with open(file_path, 'rb') as f:
                content = f.read()
            
            self.client.files.upload(remote_path, content, overwrite=True)
            logger.info(f"  ↑ Uploaded: {file_path.name}")
            return True
            
        except Exception as e:
            logger.error(f"  ✗ Upload failed {file_path.name}: {str(e)[:60]}")
            return False
    
    def upload_files(self, output_dir: str, timestamp: str = None) -> dict:
        """Upload all CSV files from output_dir to the volume."""
        self.ensure_volume_exists()
        
        # Volume path
        volume_path = self.get_volume_path()
        
        uploaded_files = []
        failed_files = []
        
        # Get all CSV files from output directory
        output_path = Path(output_dir)
        csv_files = list(output_path.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {output_dir}")
            return {"uploaded": [], "failed": [], "volume_path": volume_path}
        
        logger.info(f"Uploading {len(csv_files)} files to {volume_path}")
        
        for csv_file in csv_files:
            try:
                remote_path = f"{volume_path}/{csv_file.name}"
                
                # Read file content
                with open(csv_file, 'rb') as f:
                    content = f.read()
                
                # Upload using Files API
                self.client.files.upload(remote_path, content, overwrite=True)
                uploaded_files.append(csv_file.name)
                logger.debug(f"  Uploaded: {csv_file.name}")
                
            except Exception as e:
                failed_files.append({"file": csv_file.name, "error": str(e)})
                logger.error(f"  Failed to upload {csv_file.name}: {e}")
        
        logger.info(f"Upload complete: {len(uploaded_files)} succeeded, {len(failed_files)} failed")
        logger.info(f"Files available at: {volume_path}")
        
        return {
            "uploaded": uploaded_files,
            "failed": failed_files,
            "volume_path": volume_path,
            "volume_full_name": f"{self.catalog}.{self.schema}.{self.volume_name}"
        }

