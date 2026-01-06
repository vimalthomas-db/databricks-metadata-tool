import logging
from typing import List, Optional
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.databricks import AzureDatabricksManagementClient
from azure.mgmt.resource import ResourceManagementClient

from databricks_metadata_tool.models import Workspace

logger = logging.getLogger('databricks_metadata_tool.azure_provider')


class AzureProvider:
    
    def __init__(
        self,
        subscription_id: str,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None
    ):
        self.subscription_id = subscription_id
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        
        self.credential = self._get_credential()
        
        self.databricks_client = AzureDatabricksManagementClient(
            credential=self.credential,
            subscription_id=self.subscription_id
        )
        
        self.resource_client = ResourceManagementClient(
            credential=self.credential,
            subscription_id=self.subscription_id
        )
        
        logger.info("Azure Provider initialized")
    
    def _get_credential(self):
        if self.tenant_id and self.client_id and self.client_secret:
            logger.info("Using Service Principal authentication")
            return ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
        else:
            logger.info("Using Default Azure Credential")
            return DefaultAzureCredential()
    
    def get_access_token(self) -> str:
        databricks_resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
        token = self.credential.get_token(f"{databricks_resource_id}/.default")
        return token.token
    
    def list_workspaces(self, resource_group: Optional[str] = None) -> List[Workspace]:
        logger.info("Discovering Databricks workspaces...")
        
        workspaces = []
        
        try:
            # Treat empty string as None
            if resource_group and resource_group.strip():
                logger.info(f"Filtering by resource group: {resource_group}")
                workspace_list = self.databricks_client.workspaces.list_by_resource_group(
                    resource_group_name=resource_group
                )
            else:
                logger.warning("No resource_group filter specified - scanning ALL workspaces in subscription!")
                logger.warning("This can be slow and may hit authorization errors. Consider setting 'resource_group' in config.yaml")
                workspace_list = self.databricks_client.workspaces.list_by_subscription()
            
            for ws in workspace_list:
                workspace_url = ""
                if hasattr(ws, 'properties') and hasattr(ws.properties, 'workspace_url'):
                    workspace_url = f"https://{ws.properties.workspace_url}"
                elif hasattr(ws, 'workspace_url'):
                    workspace_url = f"https://{ws.workspace_url}"
    
                sku_name = None
                if hasattr(ws, 'sku') and ws.sku and hasattr(ws.sku, 'name'):
                    sku_name = ws.sku.name
                
                # Extract CMK (Customer Managed Keys) configuration
                cmk_info = self._extract_cmk_info(ws)
    
                workspace = Workspace(
                    workspace_id=ws.id,
                    workspace_name=ws.name,
                    workspace_url=workspace_url,
                    resource_group=self._extract_resource_group(ws.id),
                    location=ws.location,
                    subscription_id=self.subscription_id,
                    sku=sku_name,
                    cmk_managed_services_enabled=cmk_info.get('managed_services', False),
                    cmk_managed_disk_enabled=cmk_info.get('managed_disk', False),
                    cmk_dbfs_enabled=cmk_info.get('dbfs', False),
                    cmk_key_vault_uri=cmk_info.get('key_vault_uri'),
                    cmk_key_name=cmk_info.get('key_name')
                )

                workspaces.append(workspace)
                logger.debug(f"Found workspace: {workspace.workspace_name} in {workspace.location}")
        
        except Exception as e:
            logger.error(f"Error listing workspaces: {str(e)}")
            raise
        
        logger.info(f"Discovered {len(workspaces)} workspace(s)")
        return workspaces
    
    def _extract_resource_group(self, resource_id: str) -> str:
        parts = resource_id.split('/')
        try:
            rg_index = parts.index('resourceGroups') + 1
            return parts[rg_index]
        except (ValueError, IndexError):
            return "unknown"
    
    def _extract_cmk_info(self, ws) -> dict:
        """Extract CMK config: managed services, managed disk, and DBFS encryption"""
        cmk_info = {
            'managed_services': False,
            'managed_disk': False,
            'dbfs': False,
            'key_vault_uri': None,
            'key_name': None
        }
        
        try:
            # Check if encryption properties exist
            if not hasattr(ws, 'properties') or not ws.properties:
                return cmk_info
            
            props = ws.properties
            
            # Check for encryption entities
            if hasattr(props, 'encryption') and props.encryption:
                encryption = props.encryption
                
                if hasattr(encryption, 'entities') and encryption.entities:
                    entities = encryption.entities
                    
                    # Managed Services CMK
                    if hasattr(entities, 'managed_services') and entities.managed_services:
                        ms = entities.managed_services
                        if hasattr(ms, 'key_source') and ms.key_source == 'Microsoft.Keyvault':
                            cmk_info['managed_services'] = True
                            if hasattr(ms, 'key_vault_properties') and ms.key_vault_properties:
                                kv = ms.key_vault_properties
                                cmk_info['key_vault_uri'] = getattr(kv, 'key_vault_uri', None)
                                cmk_info['key_name'] = getattr(kv, 'key_name', None)
                    
                    # Managed Disk CMK
                    if hasattr(entities, 'managed_disk') and entities.managed_disk:
                        md = entities.managed_disk
                        if hasattr(md, 'key_source') and md.key_source == 'Microsoft.Keyvault':
                            cmk_info['managed_disk'] = True
                            if not cmk_info['key_vault_uri'] and hasattr(md, 'key_vault_properties') and md.key_vault_properties:
                                kv = md.key_vault_properties
                                cmk_info['key_vault_uri'] = getattr(kv, 'key_vault_uri', None)
                                cmk_info['key_name'] = getattr(kv, 'key_name', None)
            
            # Check for DBFS CMK (prepareEncryption parameter)
            if hasattr(props, 'parameters') and props.parameters:
                params = props.parameters
                if hasattr(params, 'prepare_encryption') and params.prepare_encryption:
                    pe = params.prepare_encryption
                    if hasattr(pe, 'value') and pe.value:
                        cmk_info['dbfs'] = str(pe.value).lower() == 'true'
            
            if cmk_info['managed_services'] or cmk_info['managed_disk'] or cmk_info['dbfs']:
                logger.debug(f"CMK detected for workspace: managed_services={cmk_info['managed_services']}, managed_disk={cmk_info['managed_disk']}, dbfs={cmk_info['dbfs']}")
                
        except Exception as e:
            logger.debug(f"Could not extract CMK info: {str(e)}")
        
        return cmk_info
    
    def test_connection(self) -> bool:
        try:
            list(self.resource_client.resource_groups.list())
            logger.info("Azure connection test successful")
            return True
        except Exception as e:
            logger.error(f"Azure connection test failed: {str(e)}")
            return False