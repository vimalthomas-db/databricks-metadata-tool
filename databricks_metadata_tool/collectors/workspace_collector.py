"""Workspace-level metadata collector with multi-cloud auth support."""

import logging
import os
from typing import List, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo

from databricks_metadata_tool.models import Workspace, Catalog, ExternalLocation, CatalogBinding, Share, SharedObject, Repo, WorkspaceConfig
from databricks_metadata_tool.utils import extract_storage_info

logger = logging.getLogger('databricks_metadata_tool.workspace_collector')


class WorkspaceCollector:
    """Collector for workspace-level metadata."""
    
    WORKSPACE_TYPE_UC = "UNITY_CATALOG"
    WORKSPACE_TYPE_HMS = "HIVE_METASTORE"
    
    def __init__(self, workspace: Workspace):
        self.workspace = workspace
        self.auth_method = self._get_auth_method()
        self.client = self._create_client()
        
        # Detect workspace type
        self.workspace_type = self._detect_workspace_type()
        logger.info(f"Workspace {workspace.workspace_name} type: {self.workspace_type} (auth={self.auth_method})")
    
    def _get_auth_method(self) -> str:
        """Determine authentication method from environment"""
        if os.getenv('DATABRICKS_CLIENT_ID') and os.getenv('DATABRICKS_CLIENT_SECRET'):
            return 'oauth_m2m'
        if os.getenv('AZURE_CLIENT_ID') and os.getenv('AZURE_CLIENT_SECRET'):
            return 'azure_sp'
        raise ValueError("No valid credentials found")
    
    def _create_client(self) -> WorkspaceClient:
        """Create WorkspaceClient with appropriate auth"""
        if self.auth_method == 'oauth_m2m':
            return WorkspaceClient(
                host=self.workspace.workspace_url,
                client_id=os.getenv('DATABRICKS_CLIENT_ID'),
                client_secret=os.getenv('DATABRICKS_CLIENT_SECRET')
            )
        else:  # azure_sp
            return WorkspaceClient(
                host=self.workspace.workspace_url,
                azure_client_id=os.getenv('AZURE_CLIENT_ID'),
                azure_tenant_id=os.getenv('AZURE_TENANT_ID'),
                azure_client_secret=os.getenv('AZURE_CLIENT_SECRET')
            )
    
    def _detect_workspace_type(self) -> str:
        try:
            current_metastore = self.client.metastores.current()
            if current_metastore and current_metastore.metastore_id:
                # Store metastore info for later use
                self.metastore_id = current_metastore.metastore_id
                try:
                    metastore_details = self.client.metastores.get(current_metastore.metastore_id)
                    self.metastore_name = metastore_details.name if hasattr(metastore_details, 'name') else current_metastore.metastore_id
                except:
                    self.metastore_name = current_metastore.metastore_id
                return self.WORKSPACE_TYPE_UC
        except Exception as e:
            error_msg = str(e).lower()
            if 'blocked' in error_msg or 'ip' in error_msg or 'acl' in error_msg:
                logger.warning(f"IP ACL may be blocking access: {str(e)[:100]}")
            elif 'unauthorized' in error_msg or '403' in error_msg:
                logger.warning(f"Access denied to workspace: {str(e)[:100]}")
            else:
                logger.debug(f"No Unity Catalog metastore found: {str(e)[:100]}")
        
        self.metastore_id = None
        self.metastore_name = None
        return self.WORKSPACE_TYPE_HMS
    
    def test_connection(self) -> bool:
        try:
            current_user = self.client.current_user.me()
            logger.info(f"Connected to workspace as: {current_user.user_name}")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def list_catalogs(self) -> List[Catalog]:
        logger.info(f"Listing catalogs in workspace: {self.workspace.workspace_name}")
        
        catalogs = []
        
        if self.workspace_type == self.WORKSPACE_TYPE_HMS:
            # Legacy HMS workspace - collect HMS data
            logger.info("  Workspace uses Hive Metastore (legacy) - collecting HMS data")
            hms_catalogs = self._collect_hms_legacy()
            catalogs.extend(hms_catalogs)
        else:
            # Unity Catalog workspace
            logger.info("  Workspace uses Unity Catalog")
            uc_catalogs = self._collect_uc_catalogs()
            catalogs.extend(uc_catalogs)
            
            # Also check for HMS data within UC workspace
            hms_in_uc = self._collect_hms_in_uc()
            if hms_in_uc:
                logger.info(f"  Found legacy HMS data in UC workspace")
                catalogs.extend(hms_in_uc)
        
        logger.info(f"Found {len(catalogs)} catalog(s) total")
        return catalogs
    
    def _collect_uc_catalogs(self) -> List[Catalog]:
        catalogs = []
        
        # Reuse metastore info from workspace detection
        metastore_id = self.metastore_id
        metastore_name = self.metastore_name
        logger.debug(f"  Using metastore: {metastore_name} ({metastore_id})")
        
        try:
            catalog_list = self.client.catalogs.list()
            
            for cat in catalog_list:
                # Skip hive_metastore catalog here - will be handled separately
                if cat.name == "hive_metastore":
                    continue
                
                catalog = Catalog(
                    catalog_name=cat.name,
                    catalog_type=cat.catalog_type.value if cat.catalog_type else "UNKNOWN",
                    owner=cat.owner,
                    comment=cat.comment,
                    properties=dict(cat.properties) if cat.properties else {},
                    created_at=cat.created_at,
                    created_by=cat.created_by,
                    updated_at=cat.updated_at,
                    updated_by=cat.updated_by,
                    workspace_id=self.workspace.workspace_id,
                    workspace_name=self.workspace.workspace_name,
                    metastore_id=metastore_id,
                    metastore_name=metastore_name,
                    is_legacy_hms=False
                )
                catalogs.append(catalog)
                logger.debug(f"  Found UC catalog: {catalog.catalog_name}")
        
        except Exception as e:
            logger.error(f"Error listing UC catalogs: {str(e)}")
            raise
        
        return catalogs
    
    def _collect_hms_legacy(self) -> List[Catalog]:
        catalogs = []
        
        try:
            # In legacy workspace, try to access schemas (databases)
            # This represents HMS databases
            logger.info("  Attempting to collect HMS databases from legacy workspace")
            
            # Create a pseudo-catalog representing HMS
            hms_catalog = Catalog(
                catalog_name="hive_metastore",
                catalog_type="HIVE_METASTORE",
                owner="N/A",
                comment="Legacy Hive Metastore workspace (not UC-enabled)",
                properties={"workspace_type": "HIVE_METASTORE_ONLY"},
                workspace_id=self.workspace.workspace_id,
                workspace_name=self.workspace.workspace_name,
                metastore_id=None,
                metastore_name="hive_metastore",
                is_legacy_hms=True
            )
            catalogs.append(hms_catalog)
            logger.info("  Created HMS catalog entry for legacy workspace")
            
        except Exception as e:
            logger.warning(f"Could not collect HMS data from legacy workspace: {str(e)}")
        
        return catalogs
    
    def _collect_hms_in_uc(self) -> List[Catalog]:
        catalogs = []
        
        try:
            # Check if hive_metastore catalog exists (indicates HMS data present)
            catalog_list = self.client.catalogs.list()
            
            has_hms = any(cat.name == "hive_metastore" for cat in catalog_list)
            
            if has_hms:
                logger.info("  Found hive_metastore catalog - legacy HMS data present")
                
                # Try to list schemas to verify accessibility
                try:
                    schemas = list(self.client.schemas.list(catalog_name="hive_metastore"))
                    schema_count = len(schemas)
                    logger.info(f"  hive_metastore has {schema_count} schemas (HMS databases)")
                    
                    hms_catalog = Catalog(
                        catalog_name="hive_metastore",
                        catalog_type="HIVE_METASTORE",
                        owner="N/A",
                        comment=f"Legacy HMS data in UC workspace ({schema_count} databases)",
                        properties={
                            "workspace_type": "UNITY_CATALOG_WITH_HMS",
                            "hms_schema_count": str(schema_count)
                        },
                        workspace_id=self.workspace.workspace_id,
                        workspace_name=self.workspace.workspace_name,
                        metastore_id=self.metastore_id,
                        metastore_name=self.metastore_name,
                        is_legacy_hms=True
                    )
                    catalogs.append(hms_catalog)
                    
                except Exception as e:
                    logger.debug(f"  hive_metastore catalog exists but not accessible: {str(e)}")
            
        except Exception as e:
            logger.debug(f"Could not check for HMS data in UC workspace: {str(e)}")
        
        return catalogs
    
    def list_catalog_bindings(self, catalogs: List[Catalog], metastore_id: str, metastore_name: str) -> List[CatalogBinding]:
        logger.info(f"Listing catalog workspace bindings in workspace: {self.workspace.workspace_name}")
        
        bindings = []
        
        try:
            for catalog in catalogs:
                try:
                    # Get workspace bindings for this catalog
                    binding_list = self.client.workspace_bindings.get(catalog.catalog_name)
                    
                    if binding_list and hasattr(binding_list, 'workspaces') and binding_list.workspaces:
                        for workspace_binding in binding_list.workspaces:
                            # Handle case where workspace_binding is an int (workspace ID directly)
                            # or an object with workspace_id attribute
                            if isinstance(workspace_binding, int):
                                workspace_id = str(workspace_binding)
                                binding_type = 'BINDING_READ_WRITE'  # Default for simple bindings
                            else:
                                workspace_id = str(workspace_binding.workspace_id) if hasattr(workspace_binding, 'workspace_id') else str(workspace_binding)
                                binding_type = workspace_binding.binding_type.value if hasattr(workspace_binding, 'binding_type') else 'BINDING_READ_WRITE'
                            
                            binding = CatalogBinding(
                                catalog_name=catalog.catalog_name,
                                workspace_id=workspace_id,
                                binding_type=binding_type,
                                metastore_id=metastore_id,
                                metastore_name=metastore_name
                            )
                            bindings.append(binding)
                            logger.info(f"  Catalog {catalog.catalog_name} bound to workspace {workspace_id} ({binding_type})")
                
                except Exception as e:
                    logger.warning(f"  Could not get bindings for catalog {catalog.catalog_name}: {str(e)}")
                    # Continue to next catalog
                    continue
        
        except Exception as e:
            logger.warning(f"Error listing catalog bindings: {str(e)}")
        
        logger.info(f"Found {len(bindings)} catalog binding(s)")
        return bindings
    
    def get_workspace_info(self) -> dict:
        try:
            current_user = self.client.current_user.me()
            
            return {
                'workspace_name': self.workspace.workspace_name,
                'workspace_url': self.workspace.workspace_url,
                'current_user': current_user.user_name,
                'user_id': current_user.id
            }
        except Exception as e:
            logger.error(f"Error getting workspace info: {str(e)}")
            return {}
    
    def list_external_locations(self) -> List[ExternalLocation]:
        logger.info(f"Listing external locations in workspace: {self.workspace.workspace_name}")
        
        external_locations = []
        
        try:
            location_list = self.client.external_locations.list()
            
            for loc in location_list:
                storage_info = extract_storage_info(loc.url)
                
                external_location = ExternalLocation(
                    location_name=loc.name,
                    url=loc.url,
                    credential_name=loc.credential_name if hasattr(loc, 'credential_name') else None,
                    owner=loc.owner,
                    comment=loc.comment,
                    read_only=loc.read_only if hasattr(loc, 'read_only') else False,
                    created_at=loc.created_at,
                    created_by=loc.created_by,
                    updated_at=loc.updated_at,
                    updated_by=loc.updated_by,
                    workspace_id=self.workspace.workspace_id,
                    workspace_name=self.workspace.workspace_name,
                    storage_account=storage_info['storage_account'],
                    storage_type=storage_info['storage_type']
                )
                external_locations.append(external_location)
                logger.debug(f"  Found external location: {loc.name} -> {storage_info['storage_account']}")
        
        except Exception as e:
            logger.warning(f"Error listing external locations: {str(e)}")
            logger.debug(f"External locations error details:", exc_info=True)
        
        logger.info(f"Found {len(external_locations)} external location(s)")
        return external_locations
    
    def list_shares(self) -> List[Share]:
        logger.info(f"Listing Delta shares in workspace: {self.workspace.workspace_name}")
        
        shares = []
        
        try:
            logger.debug("Calling client.shares.list()...")
            share_list = list(self.client.shares.list())
            logger.debug(f"  API returned {len(share_list)} share(s)")
            
            for sh in share_list:
                share = Share(
                    share_name=sh.name,
                    owner=sh.owner if hasattr(sh, 'owner') else None,
                    comment=sh.comment if hasattr(sh, 'comment') else None,
                    created_at=sh.created_at if hasattr(sh, 'created_at') else None,
                    created_by=sh.created_by if hasattr(sh, 'created_by') else None,
                    updated_at=sh.updated_at if hasattr(sh, 'updated_at') else None,
                    updated_by=sh.updated_by if hasattr(sh, 'updated_by') else None,
                    workspace_id=self.workspace.workspace_id,
                    workspace_name=self.workspace.workspace_name
                )
                
                # Get share details (objects included in share)
                try:
                    share_details = self.client.shares.get(sh.name)
                    if share_details and hasattr(share_details, 'objects') and share_details.objects:
                        for obj in share_details.objects:
                            shared_obj = SharedObject(
                                name=obj.name,
                                data_object_type=obj.data_object_type.value if hasattr(obj, 'data_object_type') and obj.data_object_type else 'UNKNOWN',
                                status=obj.status.value if hasattr(obj, 'status') and obj.status else None,
                                added_at=obj.added_at if hasattr(obj, 'added_at') else None,
                                added_by=obj.added_by if hasattr(obj, 'added_by') else None,
                                cdf_enabled=obj.cdf_enabled if hasattr(obj, 'cdf_enabled') else False,
                                history_data_sharing_status=obj.history_data_sharing_status.value if hasattr(obj, 'history_data_sharing_status') and obj.history_data_sharing_status else None,
                                start_version=obj.start_version if hasattr(obj, 'start_version') else None,
                                shared_as=obj.shared_as if hasattr(obj, 'shared_as') else None
                            )
                            share.objects.append(shared_obj)
                            share.shared_tables.append(obj.name)
                            
                            # Count by type
                            if shared_obj.data_object_type == 'TABLE':
                                share.table_count += 1
                            elif shared_obj.data_object_type == 'SCHEMA':
                                share.schema_count += 1
                        
                        share.object_count = len(share.objects)
                        logger.debug(f"    Share {sh.name}: {share.table_count} tables, {share.schema_count} schemas")
                except Exception as e:
                    logger.debug(f"    Could not get share details for {sh.name}: {str(e)}")
                
                # Get share recipients
                try:
                    share_permissions = self.client.shares.share_permissions(sh.name)
                    if share_permissions and hasattr(share_permissions, 'privilege_assignments'):
                        for perm in share_permissions.privilege_assignments:
                            if hasattr(perm, 'principal') and perm.principal:
                                share.recipients.append(perm.principal)
                        share.recipient_count = len(share.recipients)
                        logger.debug(f"    Share {sh.name}: {share.recipient_count} recipient(s)")
                except Exception as e:
                    logger.debug(f"    Could not get recipients for {sh.name}: {str(e)}")
                
                shares.append(share)
                logger.debug(f"  Found share: {sh.name}")
        
        except Exception as e:
            logger.warning(f"Error listing shares: {str(e)}")
        
        logger.info(f"Found {len(shares)} share(s)")
        return shares
    
    def list_repos(self) -> List[Repo]:
        logger.info(f"Listing Git repos in workspace: {self.workspace.workspace_name}")
        
        repos = []
        
        try:
            logger.debug("Calling client.repos.list()...")
            repo_list = self.client.repos.list()
            
            # Convert to list to see if it's empty
            repo_list_items = list(repo_list)
            logger.debug(f"API returned {len(repo_list_items)} items")
            
            for r in repo_list_items:
                repo = Repo(
                    repo_id=r.id,
                    repo_path=r.path,
                    url=r.url,
                    provider=r.provider,
                    branch=r.branch if hasattr(r, 'branch') else None,
                    head_commit_id=r.head_commit_id if hasattr(r, 'head_commit_id') else None,
                    workspace_id=self.workspace.workspace_id,
                    workspace_name=self.workspace.workspace_name
                )
                repos.append(repo)
                logger.debug(f"  Found repo: {r.path} ({r.provider})")
        
        except Exception as e:
            logger.error(f"Error listing repos: {str(e)}", exc_info=True)
        
        logger.info(f"Found {len(repos)} repo(s)")
        
        if len(repos) == 0:
            logger.debug("No repos found in workspace")
        
        return repos
    
    def list_workspace_configs(self) -> List[WorkspaceConfig]:
        logger.info(f"Listing workspace configurations in workspace: {self.workspace.workspace_name}")
        
        configs = []
        
        try:
            config_keys = [
                'enableIpAccessLists',
                'enableTokensConfig',
                'maxTokenLifetimeDays',
                'enableDeprecatedGlobalInitScripts',
                'enableDeprecatedClusterNamedInitScripts',
                'enableDcs',
                'enableResultsDownloading',
                'enableWebTerminal',
                'storeInteractiveNotebookResultsInCustomerAccount',
                'enableProjectTypeInWorkspace',
                'enforceUserIsolation',
                'enableGp3',
                'intercomAdminConsent'
            ]
            
            for key in config_keys:
                try:
                    value = self.client.workspace_conf.get_status(keys=key)
                    if value:
                        config = WorkspaceConfig(
                            config_key=key,
                            config_value=str(value),
                            workspace_id=self.workspace.workspace_id,
                            workspace_name=self.workspace.workspace_name
                        )
                        configs.append(config)
                        logger.debug(f"  Found config: {key} = {value}")
                except Exception:
                    pass
        
        except Exception as e:
            logger.warning(f"Error listing workspace configs: {str(e)}")
        
        logger.info(f"Found {len(configs)} workspace configuration(s)")
        return configs
    
    def get_workspace_features(self) -> dict:
        """Get workspace features/settings as a dictionary for workspace enrichment"""
        import requests
        
        config_keys_to_query = [
            "enableTokensConfig", "maxTokenLifetimeDays", "enforceUserIsolation",
            "storeInteractiveNotebookResultsInCustomerAccount", "enableIpAccessLists",
            "enableResultsDownloading", "enableWebTerminal", "enableDbfsFileBrowser",
            "enableExportNotebook", "enableNotebookTableClipboard", "enableProjectTypeInWorkspace",
            "enableDeprecatedGlobalInitScripts", "enableDcs", "enableJobViewAcls",
            "enableWorkspaceFilesystem",
        ]
        
        features = {}
        
        # Use REST API directly (SDK has bug with workspace_conf.get_status)
        try:
            auth_header = dict(self.client.config.authenticate())
            ws_url = self.workspace.workspace_url.rstrip('/')
            
            for config_key in config_keys_to_query:
                try:
                    url = f"{ws_url}/api/2.0/workspace-conf"
                    response = requests.get(url, headers=auth_header, params={"keys": config_key}, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        value = data.get(config_key)
                        
                        # Only include if value is set
                        if value is not None and str(value).lower() not in ['none', '']:
                            if str(value).lower() in ['true', 'false']:
                                features[config_key] = str(value).lower() == 'true'
                            elif str(value).isdigit():
                                features[config_key] = int(value)
                            else:
                                features[config_key] = str(value)
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"Error getting workspace config: {e}")
        
        # Check for serverless compute (check if any serverless warehouses exist)
        try:
            warehouses = list(self.client.warehouses.list())
            serverless_count = sum(1 for wh in warehouses 
                                   if hasattr(wh, 'enable_serverless_compute') and wh.enable_serverless_compute)
            features['serverless_warehouses_count'] = serverless_count
            features['serverless_compute_enabled'] = serverless_count > 0
        except Exception:
            features['serverless_compute_enabled'] = False
        
        # Check for model serving (count endpoints)
        try:
            endpoints = list(self.client.serving_endpoints.list())
            features['model_serving_endpoints_count'] = len(endpoints)
            features['model_serving_enabled'] = len(endpoints) > 0
        except Exception:
            features['model_serving_enabled'] = False
        
        # Check for SQL warehouses
        try:
            warehouses = list(self.client.warehouses.list())
            features['sql_warehouses_count'] = len(warehouses)
        except Exception:
            pass
        
        # Check for clusters
        try:
            clusters = list(self.client.clusters.list())
            features['clusters_count'] = len(clusters)
        except Exception:
            pass
        
        enabled_count = sum(1 for k, v in features.items() if v is True)
        logger.info(f"  Collected {len(features)} feature(s), {enabled_count} enabled")
        return features
    
    def get_workspace_admins(self) -> dict:
        """Get admin users and groups for the workspace"""
        logger.info(f"Collecting admin info for workspace: {self.workspace.workspace_name}")
        
        # Only these entitlements indicate admin-level privileges
        # Note: workspace-access and databricks-sql-access are NOT admin indicators
        ADMIN_ENTITLEMENTS = ['allow-cluster-create', 'allow-instance-pool-create']
        
        result = {
            'admin_users': [],
            'admin_groups': [],
            'admin_user_count': 0,
            'admin_group_count': 0
        }
        
        try:
            groups = list(self.client.groups.list())
            admin_groups = []
            
            for group in groups:
                # Check if group name contains 'admin'
                if group.display_name and 'admin' in group.display_name.lower():
                    admin_groups.append(group.display_name)
                    continue
                    
                # Check for admin-level entitlements
                if hasattr(group, 'entitlements') and group.entitlements:
                    for entitlement in group.entitlements:
                        if hasattr(entitlement, 'value') and entitlement.value in ADMIN_ENTITLEMENTS:
                            if group.display_name not in admin_groups:
                                admin_groups.append(group.display_name)
                            break
            
            result['admin_groups'] = admin_groups
            result['admin_group_count'] = len(admin_groups)
            
        except Exception as e:
            logger.debug(f"Could not list groups: {str(e)}")
        
        try:
            users = list(self.client.users.list())
            admin_users = []
            
            for user in users:
                # Check for admin-level entitlements
                if hasattr(user, 'entitlements') and user.entitlements:
                    for entitlement in user.entitlements:
                        if hasattr(entitlement, 'value') and entitlement.value in ADMIN_ENTITLEMENTS:
                            if user.display_name:
                                admin_users.append(user.display_name)
                            elif user.user_name:
                                admin_users.append(user.user_name)
                            break
            
            result['admin_users'] = admin_users
            result['admin_user_count'] = len(admin_users)
            
        except Exception as e:
            logger.debug(f"Could not list users: {str(e)}")
        
        logger.info(f"  Found {result['admin_group_count']} admin group(s), {result['admin_user_count']} admin user(s)")
        return result
    
    def get_ip_access_lists(self) -> dict:
        """Get IP access list configuration"""
        logger.info(f"Checking IP access lists for workspace: {self.workspace.workspace_name}")
        
        result = {
            'ip_access_list_enabled': False,
            'ip_access_list_count': 0,
            'ip_access_lists': []
        }
        
        try:
            # Check if IP access lists are enabled (use direct API - SDK method is broken)
            config = self.client.api_client.do('GET', '/api/2.0/workspace-conf', query={'keys': 'enableIpAccessLists'})
            if config:
                result['ip_access_list_enabled'] = str(config.get('enableIpAccessLists', '')).lower() == 'true'
        except Exception as e:
            logger.debug(f"Could not check IP access list config: {str(e)}")
        
        try:
            # List IP access lists
            ip_lists = list(self.client.ip_access_lists.list())
            result['ip_access_list_count'] = len(ip_lists)
            
            for ip_list in ip_lists:
                result['ip_access_lists'].append({
                    'name': ip_list.label if hasattr(ip_list, 'label') else 'unknown',
                    'list_type': ip_list.list_type.value if hasattr(ip_list, 'list_type') and ip_list.list_type else 'unknown',
                    'enabled': ip_list.enabled if hasattr(ip_list, 'enabled') else False,
                    'ip_count': len(ip_list.ip_addresses) if hasattr(ip_list, 'ip_addresses') and ip_list.ip_addresses else 0
                })
        except Exception as e:
            logger.debug(f"Could not list IP access lists: {str(e)}")
        
        logger.info(f"  IP Access Lists: enabled={result['ip_access_list_enabled']}, count={result['ip_access_list_count']}")
        return result
    
    def _get_ncc_via_account_api(self, account_id: str, result: dict) -> dict:
        """
        Get NCC info via Databricks Account API - only for THIS workspace.
        Supports both OAuth M2M and Azure SP authentication.
        """
        from databricks.sdk import AccountClient
        
        # Determine account host based on cloud
        cloud = getattr(self.workspace, 'cloud', 'azure')
        account_hosts = {
            'azure': 'https://accounts.azuredatabricks.net',
            'aws': 'https://accounts.cloud.databricks.com',
            'gcp': 'https://accounts.gcp.databricks.com',
        }
        account_host = account_hosts.get(cloud, account_hosts['azure'])
        
        # Create AccountClient with same auth method as workspace client
        if self.auth_method == 'oauth_m2m':
            account_client = AccountClient(
                host=account_host,
                account_id=account_id,
                client_id=os.getenv('DATABRICKS_CLIENT_ID'),
                client_secret=os.getenv('DATABRICKS_CLIENT_SECRET')
            )
        else:  # azure_sp
            account_client = AccountClient(
                host=account_host,
                account_id=account_id,
                azure_client_id=os.getenv('AZURE_CLIENT_ID'),
                azure_tenant_id=os.getenv('AZURE_TENANT_ID'),
                azure_client_secret=os.getenv('AZURE_CLIENT_SECRET')
            )
        
        # Get all NCCs and find the one attached to this workspace
        nccs = list(account_client.network_connectivity.list_network_connectivity_configurations())
        
        workspace_region = getattr(self.workspace, 'cloud_region', None) or getattr(self.workspace, 'location', '')
        workspace_ncc = None
        
        for ncc in nccs:
            if ncc.region and workspace_region and ncc.region.lower() == workspace_region.lower():
                workspace_ncc = ncc
                break
        
        if workspace_ncc:
            result['ncc_name'] = workspace_ncc.name
            result['ncc_id'] = workspace_ncc.network_connectivity_config_id
            result['detection_method'] = 'account_api'
            result['private_access_enabled'] = True
            
            if workspace_ncc.egress_config and workspace_ncc.egress_config.target_rules:
                rules = workspace_ncc.egress_config.target_rules
                if rules.azure_private_endpoint_rules:
                    for rule in rules.azure_private_endpoint_rules:
                        result['serverless_private_endpoint_count'] += 1
                        result['private_endpoints'].append({
                            'rule_id': rule.rule_id,
                            'target_resource': rule.resource_id.split('/')[-1] if rule.resource_id else 'unknown',
                            'resource_type': rule.group_id or 'unknown',
                            'status': str(rule.connection_state) if rule.connection_state else 'Unknown',
                            'endpoint_id': rule.endpoint_name or 'unknown'
                        })
            
            logger.info(f"  NCC: {workspace_ncc.name}, PE rules: {result['serverless_private_endpoint_count']}")
        else:
            result['detection_method'] = 'account_api'
            result['private_access_enabled'] = False
            logger.debug(f"  No NCC found for workspace region {workspace_region}")
        
        return result
    
    def get_private_endpoint_info(self) -> dict:
        """
        Get serverless private endpoint and private access configuration.
        
        Uses Databricks Account API (NCC) for detection.
        Falls back to Azure storage scan only for Azure workspaces with SP auth.
        """
        logger.info(f"Checking private endpoint config for workspace: {self.workspace.workspace_name}")
        
        result = {
            'private_access_enabled': False,
            'serverless_private_endpoint_count': 0,
            'private_endpoints': [],
            'ncc_name': None,
            'ncc_id': None,
            'detection_method': 'none'
        }
        
        account_id = os.getenv('DATABRICKS_ACCOUNT_ID')
        
        # Try Account API first (works for all clouds with OAuth M2M)
        if account_id:
            try:
                result = self._get_ncc_via_account_api(account_id, result)
                if result['detection_method'] == 'account_api':
                    logger.info(f"  Private Access: enabled={result['private_access_enabled']}, endpoints={result['serverless_private_endpoint_count']} (via Account API)")
                    return result
            except Exception as e:
                logger.debug(f"Account API NCC lookup failed: {str(e)}")
        
        # Azure-only fallback: scan storage accounts for PEs
        # Only available for Azure workspaces with Azure SP authentication
        cloud = getattr(self.workspace, 'cloud', 'azure')
        if cloud == 'azure' and self.auth_method == 'azure_sp':
            subscription_id = self.workspace.subscription_id
            resource_group = self.workspace.resource_group
            
            if not subscription_id or not resource_group:
                logger.debug("Skipping Azure storage scan - no subscription_id or resource_group")
                return result
            
            result['detection_method'] = 'azure_storage_scan'
            
            try:
                from azure.identity import ClientSecretCredential
                from azure.mgmt.storage import StorageManagementClient
                
                credential = ClientSecretCredential(
                    tenant_id=os.getenv('AZURE_TENANT_ID'),
                    client_id=os.getenv('AZURE_CLIENT_ID'),
                    client_secret=os.getenv('AZURE_CLIENT_SECRET')
                )
                
                storage_client = StorageManagementClient(credential, subscription_id)
                storage_accounts = list(storage_client.storage_accounts.list_by_resource_group(resource_group))
                logger.debug(f"  Checking {len(storage_accounts)} storage account(s) in {resource_group}")
                
                for account in storage_accounts:
                    try:
                        account_details = storage_client.storage_accounts.get_properties(
                            resource_group, 
                            account.name,
                            expand='blobRestoreStatus'
                        )
                        
                        if account_details.private_endpoint_connections:
                            for pe_conn in account_details.private_endpoint_connections:
                                if pe_conn.private_endpoint and pe_conn.private_endpoint.id:
                                    pe_id = pe_conn.private_endpoint.id
                                    is_databricks = 'databricks' in pe_id.lower() or 'private-link-lm' in pe_id.lower()
                                    
                                    if is_databricks:
                                        result['serverless_private_endpoint_count'] += 1
                                        result['private_access_enabled'] = True
                                        
                                        status = 'Unknown'
                                        if pe_conn.private_link_service_connection_state:
                                            status = pe_conn.private_link_service_connection_state.status or 'Unknown'
                                        
                                        result['private_endpoints'].append({
                                            'target_resource': account.name,
                                            'resource_type': 'storage_account',
                                            'status': status,
                                            'endpoint_id': pe_id.split('/')[-1] if pe_id else 'unknown'
                                        })
                    except Exception as e:
                        logger.debug(f"Could not check storage account {account.name}: {str(e)}")
                        
            except ImportError:
                logger.debug("Azure SDK not available, skipping storage scan")
            except Exception as e:
                logger.debug(f"Could not check Azure private endpoints: {str(e)}")
        
        logger.info(f"  Private Access: enabled={result['private_access_enabled']}, endpoints={result['serverless_private_endpoint_count']}")
        return result