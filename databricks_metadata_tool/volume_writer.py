"""Upload collection output files to a Unity Catalog volume."""

import os
import logging
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType, SecurableType, PermissionsChange, Privilege

logger = logging.getLogger('databricks_metadata_tool.volume_writer')

# Default names for collection output
DEFAULT_CATALOG = "collection_catalog"
DEFAULT_SCHEMA = "collection_schema"
DEFAULT_VOLUME = "collection_volume"
STAGING_FOLDER = "staging"


class VolumeWriter:
    """Upload CSV files to a Unity Catalog volume."""
    
    def __init__(self, workspace_url: str, catalog: str = None, schema: str = None, volume: str = None):
        self.workspace_url = workspace_url
        self.catalog = catalog or DEFAULT_CATALOG
        self.schema = schema or DEFAULT_SCHEMA
        self.volume_name = volume or DEFAULT_VOLUME
        self.staging_folder = STAGING_FOLDER
        
        self.client = WorkspaceClient(host=workspace_url)
    
    def ensure_catalog_exists(self):
        """Create catalog if it doesn't exist."""
        created_new = False
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
            created_new = True
        
        if created_new:
            self._grant_catalog_permissions()
    
    def ensure_schema_exists(self):
        """Create schema if it doesn't exist."""
        schema_full_name = f"{self.catalog}.{self.schema}"
        created_new = False
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
            created_new = True
        
        if created_new:
            self._grant_schema_permissions(schema_full_name)
    
    def _grant_catalog_permissions(self):
        """Grant USE_CATALOG to account users."""
        try:
            self.client.grants.update(
                securable_type=SecurableType.CATALOG,
                full_name=self.catalog,
                changes=[
                    PermissionsChange(
                        add=[Privilege.USE_CATALOG],
                        principal="account users"
                    )
                ]
            )
            logger.info(f"  Granted USE_CATALOG on {self.catalog} to account users")
        except Exception as e:
            logger.warning(f"  Could not grant permissions on catalog: {e}")
    
    def _grant_schema_permissions(self, schema_full_name: str):
        """Grant USE_SCHEMA to account users."""
        try:
            self.client.grants.update(
                securable_type=SecurableType.SCHEMA,
                full_name=schema_full_name,
                changes=[
                    PermissionsChange(
                        add=[Privilege.USE_SCHEMA],
                        principal="account users"
                    )
                ]
            )
            logger.info(f"  Granted USE_SCHEMA on {schema_full_name} to account users")
        except Exception as e:
            logger.warning(f"  Could not grant permissions on schema: {e}")
        
    def ensure_volume_exists(self) -> str:
        """Create the full hierarchy: catalog, schema, volume."""
        self.ensure_catalog_exists()
        self.ensure_schema_exists()
        
        volume_full_name = f"{self.catalog}.{self.schema}.{self.volume_name}"
        created_new = False
        
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
            created_new = True
        
        # Grant READ and WRITE permissions to account users
        if created_new:
            self._grant_volume_permissions(volume_full_name)
        
        return volume_full_name
    
    def _grant_volume_permissions(self, volume_full_name: str):
        """Grant READ_VOLUME and WRITE_VOLUME to account users."""
        try:
            self.client.grants.update(
                securable_type=SecurableType.VOLUME,
                full_name=volume_full_name,
                changes=[
                    PermissionsChange(
                        add=[Privilege.READ_VOLUME, Privilege.WRITE_VOLUME],
                        principal="account users"
                    )
                ]
            )
            logger.info(f"  Granted READ_VOLUME, WRITE_VOLUME on {volume_full_name} to account users")
        except Exception as e:
            logger.warning(f"  Could not grant permissions on volume: {e}")
    
    def upload_files(self, output_dir: str, timestamp: str = None) -> dict:
        """Upload all CSV files from output_dir to the volume."""
        self.ensure_volume_exists()
        
        # Volume path
        volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume_name}/{self.staging_folder}"
        
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

