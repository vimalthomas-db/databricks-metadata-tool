import logging
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks_metadata_tool.cloud_provider.azure_provider import AzureProvider
from databricks_metadata_tool.cloud_provider.account_provider import AccountProvider
from databricks_metadata_tool.collectors.workspace_collector import WorkspaceCollector
from databricks_metadata_tool.collectors.catalog_collector import CatalogCollector
from databricks_metadata_tool.models import CollectionResult, ScanResult, MetastoreInfo, Workspace, Catalog, Table, Volume, Schema
from databricks_metadata_tool.utils import format_bytes
from databricks_metadata_tool.scan_exporter import (
    export_scan_to_csv, export_collection_to_csv, export_catalog_tables_csv,
    export_catalog_schemas_csv, export_catalog_volumes_csv
)


logger = logging.getLogger('databricks_metadata_tool.orchestrator')


class MetadataOrchestrator:
    """
    Databricks Metadata Orchestrator - Multi-Cloud Edition
    
    Uses Databricks Account API for workspace/metastore discovery.
    Cloud-specific IDs (subscription, AWS account, GCP project) are
    extracted from Account API - no cloud provider APIs required.
    
    Authentication (in priority order):
    1. OAuth M2M: DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (multi-cloud)
    2. Azure SP: AZURE_CLIENT_ID + AZURE_TENANT_ID + AZURE_CLIENT_SECRET
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.azure_config = config.get('azure', {})
        self.databricks_config = config.get('databricks', {})
        self.collection_config = config.get('collection', {})
        self.output_config = config.get('output', {})
        
        # Determine cloud type from config or default to azure
        self.cloud = self.databricks_config.get('cloud', 'azure')
        
        # Initialize Account Provider (required for scan/collect)
        self.account_provider = None
        account_id = self.databricks_config.get('account_id')
        if account_id:
            try:
                self.account_provider = AccountProvider(
                    account_id=account_id,
                    account_host=self.databricks_config.get('account_host'),
                    cloud=self.cloud
                )
                self.auth_method = self.account_provider.auth_method
            except Exception as e:
                logger.warning(f"Could not initialize Account Provider: {str(e)}")
                self.auth_method = 'unknown'
        else:
            self.auth_method = 'unknown'
        
        # Azure Provider is NOT initialized by default
        # Use _get_azure_provider() only if explicitly needed for enrichment
        self._azure_provider = None
        
        self.result = CollectionResult(
            subscription_id=self.databricks_config.get('account_id', 'unknown'),
            collection_timestamp=datetime.now().isoformat()
        )
        
        logger.info(f"Metadata Orchestrator initialized")
        logger.info(f"  Account ID: {account_id}")
        logger.info(f"  Auth Method: {self.auth_method}")
        logger.info(f"  Cloud: {self.cloud}")
    
    def _get_azure_provider(self):
        """Lazy-load Azure Provider only when explicitly needed for enrichment."""
        if self._azure_provider is None and self.azure_config.get('subscription_id'):
            try:
                self._azure_provider = AzureProvider(
                    subscription_id=self.azure_config['subscription_id'],
                    tenant_id=self.azure_config.get('tenant_id'),
                    client_id=self.azure_config.get('client_id'),
                    client_secret=self.azure_config.get('client_secret')
                )
                logger.info(f"Azure Provider initialized for subscription: {self.azure_config['subscription_id']}")
            except Exception as e:
                logger.warning(f"Could not initialize Azure Provider: {str(e)}")
        return self._azure_provider
    
    def collect_all(self) -> CollectionResult:
        logger.info("=" * 80)
        logger.info("Starting Databricks Metadata Collection")
        logger.info("=" * 80)
        
        try:
            workspaces = self._discover_workspaces()
            self.result.workspaces = workspaces
            
            if not workspaces:
                logger.warning("No workspaces found. Exiting.")
                return self.result
            
            max_workers = self.collection_config.get('max_workers', 5)
            
            if max_workers > 1:
                self._collect_workspaces_parallel(workspaces, max_workers)
            else:
                self._collect_workspaces_sequential(workspaces)
            
            self._log_summary()
            self._save_results()
            self._export_csv()
            
        except Exception as e:
            logger.error(f"Error in collection process: {str(e)}", exc_info=True)
            self.result.errors.append({
                'timestamp': datetime.now().isoformat(),
                'level': 'orchestrator',
                'error': str(e)
            })
        
        logger.info("=" * 80)
        logger.info("Collection completed")
        logger.info("=" * 80)
        
        return self.result
    
    def scan_workspaces(self, discovery_mode: str = None, test_workspace: str = None, 
                        deep_scan: bool = False, save_results: bool = True) -> ScanResult:
        """
        Discover workspaces and metastores (Phase 1).
        
        Args:
            discovery_mode: 'account' (default) or 'azure' (legacy, requires Azure subscription)
            test_workspace: Optional - scan only this workspace
            deep_scan: If True, verify by connecting to each workspace (slower)
                      If False (default), use only Account API (fast)
            save_results: If True (default), save scan results to files
        
        Scan Modes:
            Quick (default): Account API only - ~5-10 seconds for 100 workspaces
            Deep: Verifies via workspace connections - ~30-60 seconds for 100 workspaces
        
        Returns:
            ScanResult with discovered workspaces and metastores
        """
        mode = discovery_mode or 'account'
        
        # Use Databricks Account API (sees ALL workspaces regardless of Azure subscription)
        if not self.account_provider:
            raise ValueError(
                "Account Provider required. Set DATABRICKS_ACCOUNT_ID environment variable.\n"
                "The Account API provides complete visibility across all workspaces in your account."
            )
        result = self._scan_via_account(test_workspace=test_workspace, deep_scan=deep_scan)
        if save_results:
            # Standalone scan mode: create all account_* files including workspaces
            self._save_scan_results(result, skip_workspaces=False)
        return result
    
    def _scan_via_account(self, test_workspace: str = None, deep_scan: bool = False) -> ScanResult:
        """
        Scan using Databricks Account API.
        
        Args:
            test_workspace: Optional - scan only this workspace
            deep_scan: If True, verify by connecting to each workspace (slower)
        """
        scan_mode = "DEEP" if deep_scan else "QUICK"
        logger.info("=" * 80)
        logger.info(f"PHASE 1: ACCOUNT SCAN [{scan_mode}]")
        logger.info("=" * 80)
        
        scan_timestamp = datetime.now().isoformat()
        scan_result = ScanResult(
            subscription_id=self.databricks_config.get('account_id', 'unknown'),
            scan_timestamp=scan_timestamp
        )
        
        try:
            account_data = self.account_provider.scan_account(test_workspace=test_workspace, deep_scan=deep_scan)
            
            # Convert account workspaces to Workspace models with cloud info
            for ws_data in account_data['workspaces']:
                workspace = Workspace(
                    workspace_id=str(ws_data['workspace_id']),
                    workspace_name=ws_data['workspace_name'],
                    workspace_url=ws_data.get('workspace_url', ''),
                    
                    # Multi-cloud fields from Account API
                    cloud=ws_data.get('cloud', 'unknown'),
                    cloud_region=ws_data.get('cloud_region'),
                    workspace_status=ws_data.get('workspace_status'),
                    
                    # Azure-specific
                    subscription_id=ws_data.get('subscription_id'),
                    resource_group=ws_data.get('resource_group'),
                    
                    # AWS-specific
                    aws_account_id=ws_data.get('aws_account_id'),
                    credentials_id=ws_data.get('credentials_id'),
                    storage_configuration_id=ws_data.get('storage_configuration_id'),
                    
                    # GCP-specific
                    gcp_project_id=ws_data.get('gcp_project_id'),
                    
                    # Common
                    location=ws_data.get('cloud_region') or ws_data.get('region', ''),
                    sku=ws_data.get('pricing_tier'),
                    collected_at=scan_timestamp
                )
                
                # Classify workspace based on scan results
                if ws_data['workspace_name'] in account_data['uc_workspaces']:
                    workspace.has_unity_catalog = True
                    workspace.workspace_type = 'UNITY_CATALOG'
                    
                    # Find metastore assignment
                    for ms_id, ms_info in account_data['metastore_workspace_map'].items():
                        if ws_data['workspace_name'] in ms_info['workspace_names']:
                            workspace.metastore_id = ms_id
                            workspace.metastore_name = ms_info['metastore_name']
                            break
                elif ws_data['workspace_name'] in account_data.get('inaccessible_workspaces', []):
                    workspace.workspace_type = 'INACCESSIBLE'
                elif ws_data['workspace_name'] in account_data.get('hms_workspaces', []):
                    workspace.workspace_type = 'HIVE_METASTORE'
                else:
                    workspace.workspace_type = 'UNKNOWN'
                
                scan_result.workspaces.append(workspace)
            
            # Convert metastores
            for ms_data in account_data['metastores']:
                ms_info = MetastoreInfo(
                    metastore_id=ms_data['metastore_id'],
                    metastore_name=ms_data.get('metastore_name'),
                    region=ms_data.get('region'),
                )
                
                map_data = account_data['metastore_workspace_map'].get(ms_data['metastore_id'], {})
                ms_info.workspace_count = len(map_data.get('workspace_names', []))
                ms_info.workspaces = map_data.get('workspace_names', [])
                if ms_info.workspaces:
                    ms_info.suggested_admin_workspace = ms_info.workspaces[0]
                
                scan_result.metastores.append(ms_info)
            
            scan_result.hms_workspaces = account_data['hms_workspaces']
            scan_result.inaccessible_workspaces = account_data.get('inaccessible_workspaces', [])
            scan_result.total_workspaces = len(account_data['workspaces'])
            scan_result.uc_workspaces = len(account_data['uc_workspaces'])
            scan_result.hms_only_workspaces = len(account_data['hms_workspaces'])
            scan_result.inaccessible_count = len(scan_result.inaccessible_workspaces)
            scan_result.total_metastores = len(account_data['metastores'])
            
            # Add errors
            if account_data.get('errors'):
                scan_result.errors.extend(account_data['errors'])
            if scan_result.inaccessible_count > 0:
                logger.warning(f"Could not access {scan_result.inaccessible_count} workspace(s)")
            
            # Note: Azure enrichment (CMK, resource_group) is not done by default
            # This keeps the scan fast and subscription-agnostic
            # Use --enrich-azure flag if you need Azure-specific metadata
            
            # Don't save here - let caller decide (standalone scan vs collect)
            
        except Exception as e:
            logger.error(f"Error in account scan: {str(e)}", exc_info=True)
            scan_result.errors.append({
                'timestamp': datetime.now().isoformat(),
                'level': 'orchestrator',
                'error': str(e)
            })
        
        logger.info("=" * 80)
        logger.info("ACCOUNT SCAN completed")
        logger.info("=" * 80)
        
        return scan_result
    
    def _enrich_with_azure(self, scan_result: ScanResult):
        """Enrich scan results with Azure-specific data (CMK, networking, etc.)"""
        azure_provider = self._get_azure_provider()
        if not azure_provider:
            logger.warning("Azure enrichment skipped - no subscription_id configured")
            return
        
        try:
            azure_workspaces = azure_provider.list_workspaces()
            azure_map = {ws.workspace_name: ws for ws in azure_workspaces}
            
            enriched = 0
            for ws in scan_result.workspaces:
                if ws.workspace_name in azure_map:
                    azure_ws = azure_map[ws.workspace_name]
                    ws.resource_group = azure_ws.resource_group
                    ws.subscription_id = azure_ws.subscription_id
                    ws.cmk_managed_services_enabled = azure_ws.cmk_managed_services_enabled
                    ws.cmk_managed_disk_enabled = azure_ws.cmk_managed_disk_enabled
                    ws.cmk_dbfs_enabled = azure_ws.cmk_dbfs_enabled
                    ws.cmk_key_vault_uri = azure_ws.cmk_key_vault_uri
                    ws.cmk_key_name = azure_ws.cmk_key_name
                    enriched += 1
            
            logger.info(f"  Enriched {enriched} workspace(s) with Azure metadata")
        except Exception as e:
            logger.warning(f"  Could not enrich with Azure data: {str(e)}")
    
    def _scan_via_azure(self) -> ScanResult:
        """Scan using Azure Management API (legacy)"""
        logger.info("=" * 80)
        logger.info("PHASE 1: AZURE SCAN (Discovery)")
        logger.info("=" * 80)
        
        scan_timestamp = datetime.now().isoformat()
        scan_result = ScanResult(
            subscription_id=self.azure_config.get('subscription_id', 'unknown'),
            scan_timestamp=scan_timestamp
        )
        
        try:
            # Discover all workspaces
            workspaces = self._discover_workspaces()
            
            if not workspaces:
                logger.warning("No workspaces found.")
                return scan_result
            
            # Group workspaces by metastore
            metastore_map = {}  # metastore_id -> list of workspaces
            hms_workspaces = []
            
            for workspace in workspaces:
                # Set collected_at
                workspace.collected_at = scan_timestamp
                workspace.region = workspace.location
                
                try:
                    workspace_collector = WorkspaceCollector(workspace)
                    
                    # Update workspace with type info
                    workspace.workspace_type = workspace_collector.workspace_type
                    workspace.has_unity_catalog = (workspace_collector.workspace_type == "UNITY_CATALOG")
                    workspace.metastore_id = getattr(workspace_collector, 'metastore_id', None)
                    workspace.metastore_name = getattr(workspace_collector, 'metastore_name', None)
                    
                    # Collect workspace-level info (lightweight)
                    admin_info = workspace_collector.get_workspace_admins()
                    workspace.admin_user_count = admin_info.get('admin_user_count', 0)
                    workspace.admin_group_count = admin_info.get('admin_group_count', 0)
                    workspace.admin_users = admin_info.get('admin_users', [])
                    workspace.admin_groups = admin_info.get('admin_groups', [])
                    
                    ip_info = workspace_collector.get_ip_access_lists()
                    workspace.ip_access_list_enabled = ip_info.get('ip_access_list_enabled', False)
                    workspace.ip_access_list_count = ip_info.get('ip_access_list_count', 0)
                    
                    pe_info = workspace_collector.get_private_endpoint_info()
                    workspace.private_access_enabled = pe_info.get('private_access_enabled', False)
                    workspace.serverless_private_endpoint_count = pe_info.get('serverless_private_endpoint_count', 0)
                    
                    # Workspace features (dynamic - all accessible settings)
                    features = workspace_collector.get_workspace_features()
                    workspace.workspace_features = features  # Store all features
                    # Legacy fields for CSV compatibility
                    workspace.serverless_compute_enabled = features.get('serverless_compute_enabled', False)
                    workspace.model_serving_enabled = features.get('model_serving_enabled', False)
                    workspace.tokens_enabled = features.get('enableTokensConfig', False)
                    workspace.max_token_lifetime_days = features.get('maxTokenLifetimeDays')
                    workspace.results_downloading_enabled = features.get('enableResultsDownloading', False)
                    workspace.web_terminal_enabled = features.get('enableWebTerminal', False)
                    workspace.user_isolation_enabled = features.get('enforceUserIsolation', False)
                    
                    logger.info(f"  {workspace.workspace_name}: type={workspace.workspace_type}, metastore={workspace.metastore_id or 'None'}, features={len(features)}")
                    
                    # Group by metastore
                    if workspace.metastore_id:
                        if workspace.metastore_id not in metastore_map:
                            metastore_map[workspace.metastore_id] = {
                                'name': workspace.metastore_name,
                                'region': workspace.location,
                                'workspaces': []
                            }
                        metastore_map[workspace.metastore_id]['workspaces'].append(workspace)
                    else:
                        hms_workspaces.append(workspace.workspace_name)
                
                except Exception as e:
                    logger.error(f"Error scanning workspace {workspace.workspace_name}: {str(e)}")
                    scan_result.errors.append({
                        'timestamp': datetime.now().isoformat(),
                        'workspace': workspace.workspace_name,
                        'error': str(e)
                    })
                
                scan_result.workspaces.append(workspace)
            
            # Build metastore info list
            for metastore_id, info in metastore_map.items():
                ws_names = [ws.workspace_name for ws in info['workspaces']]
                
                # Suggest admin workspace (first premium workspace)
                suggested_admin = None
                for ws in info['workspaces']:
                    if ws.sku and ws.sku.lower() == 'premium':
                        suggested_admin = ws.workspace_name
                        break
                if not suggested_admin and ws_names:
                    suggested_admin = ws_names[0]
                
                metastore_info = MetastoreInfo(
                    metastore_id=metastore_id,
                    metastore_name=info['name'],
                    region=info['region'],
                    workspace_count=len(ws_names),
                    workspaces=ws_names,
                    suggested_admin_workspace=suggested_admin
                )
                scan_result.metastores.append(metastore_info)
            
            scan_result.hms_workspaces = hms_workspaces
            scan_result.total_workspaces = len(workspaces)
            scan_result.uc_workspaces = sum(1 for ws in workspaces if ws.has_unity_catalog)
            scan_result.hms_only_workspaces = len(hms_workspaces)
            scan_result.total_metastores = len(metastore_map)
            
            # Log summary
            self._log_scan_summary(scan_result)
            
            # Save results
            self._save_scan_results(scan_result)
            
        except Exception as e:
            logger.error(f"Error in scan process: {str(e)}", exc_info=True)
            scan_result.errors.append({
                'timestamp': datetime.now().isoformat(),
                'level': 'orchestrator',
                'error': str(e)
            })
        
        logger.info("=" * 80)
        logger.info("SCAN completed")
        logger.info("=" * 80)
        
        return scan_result
    
    def _log_scan_summary(self, scan_result: ScanResult):
        """Log Phase 1 scan summary"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("SCAN SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Subscription ID: {scan_result.subscription_id}")
        logger.info(f"Scan Time: {scan_result.scan_timestamp}")
        logger.info("-" * 80)
        logger.info(f"Total Workspaces:     {scan_result.total_workspaces}")
        logger.info(f"UC Workspaces:        {scan_result.uc_workspaces}")
        logger.info(f"HMS-Only Workspaces:  {scan_result.hms_only_workspaces}")
        logger.info(f"Inaccessible:         {scan_result.inaccessible_count}")
        logger.info(f"Total Metastores:     {scan_result.total_metastores}")
        logger.info("-" * 80)
        
        logger.info("METASTORES:")
        for ms in scan_result.metastores:
            logger.info(f"  [{ms.metastore_id}] {ms.metastore_name or 'unnamed'} ({ms.region})")
            logger.info(f"    Workspaces ({ms.workspace_count}): {', '.join(ms.workspaces)}")
            logger.info(f"    Suggested Admin: {ms.suggested_admin_workspace}")
        
        if scan_result.hms_workspaces:
            logger.info("")
            logger.info("HMS-ONLY WORKSPACES (no Unity Catalog):")
            for ws_name in scan_result.hms_workspaces:
                logger.info(f"  - {ws_name}")
        
        # Show inaccessible workspaces
        inaccessible = [ws for ws in scan_result.workspaces if ws.workspace_type == 'INACCESSIBLE']
        if inaccessible:
            logger.info("")
            logger.info(f"INACCESSIBLE WORKSPACES ({len(inaccessible)}):")
            for ws in inaccessible:
                logger.info(f"  - {ws.workspace_name}")
            logger.info(f"  (Check errors for details)")
        
        logger.info("")
        logger.info("NEXT STEP:")
        if scan_result.metastores:
            ms = scan_result.metastores[0]
            logger.info(f"  Run Phase 2 with:")
            logger.info(f"    python main.py --collect --admin-workspace {ms.suggested_admin_workspace} --warehouse-id <your-warehouse-id>")
    
    def _save_scan_results(self, scan_result: ScanResult, skip_workspaces: bool = False):
        """Save Phase 1 scan results to CSV files"""
        import os
        
        output_dir = self.output_config.get('directory', './outputs')
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        result_dict = scan_result.to_dict()
        
        # Export CSVs (skip_workspaces=True during collect, workspaces merged into collect_workspaces)
        export_scan_to_csv(result_dict, output_dir, timestamp, skip_workspaces=skip_workspaces)
        logger.info(f"\nScan results saved to: {output_dir}")
    
    def collect_from_admin(self, admin_workspace: str, warehouse_id: str, 
                          size_workers: int = 20, cluster_id: str = None,
                          size_threshold: int = 200, dry_run_sizes: bool = False) -> CollectionResult:
        """
        Phase 2: Collect detailed metadata using specified admin workspace.
        
        Collection strategy:
          - Metastore-level data (catalogs, tables, shares): From admin workspace ONLY
          - Workspace-level data (admins, repos, IP lists): From ALL workspaces in metastore
        
        Args:
            admin_workspace: Name of the admin workspace (must have metastore admin)
            warehouse_id: SQL Warehouse ID for table size queries
            size_workers: Number of parallel workers for size collection
            cluster_id: Cluster ID for Spark job fallback (optional)
            size_threshold: Table count threshold for Spark job (default: 200)
            dry_run_sizes: If True, show tier selection without collecting sizes
        """
        logger.info("=" * 80)
        logger.info("COLLECT: Full Metadata Collection")
        logger.info("=" * 80)
        
        collection_timestamp = datetime.now().isoformat()
        
        # Run account-level scan first (embedded in collect)
        logger.info("Step 1: Account-level discovery...")
        # save_results=False: don't save here, we'll save manually with skip_workspaces=True
        scan_result = self.scan_workspaces(discovery_mode='account', save_results=False)
        # Save scan results but skip account_workspaces - workspaces merged into collect_workspaces
        self._save_scan_results(scan_result, skip_workspaces=True)
        
        logger.info("-" * 80)
        logger.info("Step 2: Metastore-level collection...")
        logger.info(f"Admin Workspace: {admin_workspace}")
        logger.info(f"Warehouse ID: {warehouse_id}")
        if cluster_id:
            logger.info(f"Spark cluster: {cluster_id}")
        
        try:
            # Step 1: Find the admin workspace
            admin_ws_list = self._discover_workspaces(target_workspace=admin_workspace)
            
            if not admin_ws_list:
                all_workspaces = self._discover_workspaces()
                available = [w.workspace_name for w in all_workspaces[:10]]
                raise ValueError(f"Admin workspace '{admin_workspace}' not found. Some available: {available}")
            
            admin_ws = admin_ws_list[0]
            admin_ws.collected_at = collection_timestamp
            admin_ws.region = admin_ws.location
            
            # Connect to admin workspace
            workspace_collector = WorkspaceCollector(admin_ws)
            
            admin_ws.workspace_type = workspace_collector.workspace_type
            admin_ws.has_unity_catalog = (workspace_collector.workspace_type == "UNITY_CATALOG")
            admin_ws.metastore_id = getattr(workspace_collector, 'metastore_id', None)
            admin_ws.metastore_name = getattr(workspace_collector, 'metastore_name', None)
            
            if not admin_ws.has_unity_catalog:
                raise ValueError(f"Admin workspace '{admin_workspace}' does not have Unity Catalog enabled")
            
            logger.info(f"Connected to admin workspace: {admin_ws.workspace_name}")
            logger.info(f"Metastore: {admin_ws.metastore_name} ({admin_ws.metastore_id})")
            
            # Step 2: Get ALL workspaces assigned to this metastore (enriched)
            all_metastore_workspaces = self._get_workspaces_for_metastore(admin_ws.metastore_id)
            logger.info(f"Found {len(all_metastore_workspaces)} workspace(s) in metastore {admin_ws.metastore_name}")
            
            # Merge: All account workspaces with enriched metastore workspaces
            # Start with metastore workspaces (enriched), add remaining account workspaces
            metastore_ws_ids = {ws.workspace_id for ws in all_metastore_workspaces}
            all_workspaces = list(all_metastore_workspaces)
            
            # Add account workspaces not in this metastore (from scan)
            for ws in scan_result.workspaces:
                if ws.workspace_id not in metastore_ws_ids:
                    all_workspaces.append(ws)
            
            self.result.workspaces = all_workspaces
            logger.info(f"Total workspaces (account): {len(all_workspaces)}")
            
            # Collect metastore-level data (shares, external locations) ONCE
            logger.info("-" * 80)
            logger.info("Collecting metastore-level data...")
            logger.info("-" * 80)
            
            # Shares (metastore-level)
            shares = workspace_collector.list_shares()
            for share in shares:
                share.metastore_id = admin_ws.metastore_id
                share.metastore_name = admin_ws.metastore_name
                share.region = admin_ws.location
                share.collected_at = collection_timestamp
            self.result.shares = shares
            logger.info(f"  - Delta Shares: {len(shares)}")
            
            # External locations (metastore-level)
            external_locations = workspace_collector.list_external_locations()
            for el in external_locations:
                el.metastore_id = admin_ws.metastore_id
                el.metastore_name = admin_ws.metastore_name
                el.region = admin_ws.location
                el.collected_at = collection_timestamp
            self.result.external_locations = external_locations
            logger.info(f"  - External Locations: {len(external_locations)}")
            
            # Catalog bindings (cross-workspace view)
            exclude_catalogs = self.azure_config.get('exclude_catalogs', [])
            catalogs = workspace_collector.list_catalogs()
            catalogs_to_bind = [c for c in catalogs if c.catalog_name not in exclude_catalogs]
            
            bindings = workspace_collector.list_catalog_bindings(
                catalogs_to_bind, 
                metastore_id=admin_ws.metastore_id, 
                metastore_name=admin_ws.metastore_name
            )
            self.result.catalog_bindings = bindings
            logger.info(f"  - Catalog Bindings: {len(bindings)}")
            
            # Collect catalog/schema/table data
            logger.info("-" * 80)
            logger.info("Collecting catalog data...")
            logger.info("-" * 80)
            
            exclude_schemas = self.azure_config.get('exclude_schemas', [])
            
            # Generate timestamp for per-catalog files
            output_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_dir = self.output_config.get('directory', './outputs')
            
            catalog_table_files = []
            catalog_schema_files = []
            catalog_volume_files = []
            total_tables_saved = 0
            total_schemas_saved = 0
            total_volumes_saved = 0
            
            for catalog in catalogs:
                catalog.region = admin_ws.location
                catalog.collected_at = collection_timestamp
                
                if catalog.catalog_name in exclude_catalogs:
                    logger.info(f"  Skipping catalog: {catalog.catalog_name} (excluded)")
                    continue
                
                if catalog.is_legacy_hms:
                    logger.info(f"  Skipping catalog: {catalog.catalog_name} (HMS legacy)")
                    continue
                
                logger.info(f"  Processing catalog: {catalog.catalog_name}")
                
                catalog_collector = CatalogCollector(
                    catalog=catalog,
                    client=workspace_collector.client,
                    workspace_url=admin_ws.workspace_url,
                    exclude_schemas=exclude_schemas,
                    size_workers=size_workers
                )
                
                schemas = catalog_collector.list_schemas()
                for schema in schemas:
                    schema.region = admin_ws.location
                    schema.collected_at = collection_timestamp
                
                if schemas:
                    schema_dicts = [s.to_dict() for s in schemas]
                    schema_file = export_catalog_schemas_csv(
                        schemas=schema_dicts,
                        catalog_name=catalog.catalog_name,
                        output_dir=output_dir,
                        timestamp=output_timestamp,
                        collection_ts=collection_timestamp
                    )
                    catalog_schema_files.append(schema_file)
                    total_schemas_saved += len(schemas)
                
                if self.collection_config.get('collect_tables', True):
                    collect_sizes = self.collection_config.get('collect_sizes', True)
                    wh_id = warehouse_id if collect_sizes else None
                    
                    tables = catalog_collector.list_all_tables(
                        warehouse_id=wh_id,
                        cluster_id=cluster_id if collect_sizes else None,
                        size_threshold=size_threshold,
                        dry_run_sizes=dry_run_sizes
                    )
                    for table in tables:
                        table.region = admin_ws.location
                        table.collected_at = collection_timestamp
                    
                    if tables:
                        table_dicts = [t.to_dict() for t in tables]
                        file_path = export_catalog_tables_csv(
                            tables=table_dicts,
                            catalog_name=catalog.catalog_name,
                            output_dir=output_dir,
                            timestamp=output_timestamp,
                            collection_ts=collection_timestamp
                        )
                        catalog_table_files.append(file_path)
                        total_tables_saved += len(tables)
                
                if self.collection_config.get('collect_volumes', True):
                    volumes = catalog_collector.list_all_volumes()
                    for volume in volumes:
                        volume.region = admin_ws.location
                        volume.collected_at = collection_timestamp
                    
                    if volumes:
                        volume_dicts = [v.to_dict() for v in volumes]
                        volume_file = export_catalog_volumes_csv(
                            volumes=volume_dicts,
                            catalog_name=catalog.catalog_name,
                            output_dir=output_dir,
                            timestamp=output_timestamp,
                            collection_ts=collection_timestamp
                        )
                        catalog_volume_files.append(volume_file)
                        total_volumes_saved += len(volumes)
                
                self.result.catalogs.append(catalog)
            
            logger.info("-" * 80)
            logger.info(f"Saved: {total_schemas_saved} schemas, {total_tables_saved} tables, {total_volumes_saved} volumes")
            
            self._catalog_table_files = catalog_table_files
            self._catalog_schema_files = catalog_schema_files
            self._catalog_volume_files = catalog_volume_files
            self._total_tables_saved = total_tables_saved
            self._total_schemas_saved = total_schemas_saved
            self._total_volumes_saved = total_volumes_saved
            
            # Collect workspace-level data from ALL workspaces in metastore
            logger.info("-" * 80)
            logger.info(f"Collecting workspace-level data from {len(all_metastore_workspaces)} workspace(s)...")
            logger.info("-" * 80)
            
            for i, workspace in enumerate(all_metastore_workspaces, 1):
                workspace.collected_at = collection_timestamp
                workspace.region = workspace.location
                workspace.metastore_id = admin_ws.metastore_id
                workspace.metastore_name = admin_ws.metastore_name
                
                try:
                    ws_collector = WorkspaceCollector(workspace)
                    
                    workspace.workspace_type = ws_collector.workspace_type
                    workspace.has_unity_catalog = (ws_collector.workspace_type == "UNITY_CATALOG")
                    
                    # Admin info (workspace-specific)
                    admin_info = ws_collector.get_workspace_admins()
                    workspace.admin_user_count = admin_info.get('admin_user_count', 0)
                    workspace.admin_group_count = admin_info.get('admin_group_count', 0)
                    workspace.admin_users = admin_info.get('admin_users', [])
                    workspace.admin_groups = admin_info.get('admin_groups', [])
                    
                    # IP access lists (workspace-specific)
                    ip_info = ws_collector.get_ip_access_lists()
                    workspace.ip_access_list_enabled = ip_info.get('ip_access_list_enabled', False)
                    workspace.ip_access_list_count = ip_info.get('ip_access_list_count', 0)
                    
                    # Private endpoints (workspace-specific, uses Account API NCC)
                    pe_info = ws_collector.get_private_endpoint_info()
                    workspace.private_access_enabled = pe_info.get('private_access_enabled', False)
                    workspace.serverless_private_endpoint_count = pe_info.get('serverless_private_endpoint_count', 0)
                    
                    # Workspace features (dynamic - all accessible settings)
                    features = ws_collector.get_workspace_features()
                    workspace.workspace_features = features  # Store all features
                    # Legacy fields for CSV compatibility
                    workspace.serverless_compute_enabled = features.get('serverless_compute_enabled', False)
                    workspace.model_serving_enabled = features.get('model_serving_enabled', False)
                    workspace.tokens_enabled = features.get('enableTokensConfig', False)
                    workspace.max_token_lifetime_days = features.get('maxTokenLifetimeDays')
                    workspace.results_downloading_enabled = features.get('enableResultsDownloading', False)
                    workspace.web_terminal_enabled = features.get('enableWebTerminal', False)
                    workspace.user_isolation_enabled = features.get('enforceUserIsolation', False)
                    
                    # Git repos (workspace-specific)
                    repos = ws_collector.list_repos()
                    self.result.repos.extend(repos)
                    
                    logger.info(f"  [{i}/{len(all_metastore_workspaces)}] {workspace.workspace_name}: admins={workspace.admin_user_count}, repos={len(repos)}, serverless={workspace.serverless_compute_enabled}")
                    
                except Exception as e:
                    logger.warning(f"  [{i}/{len(all_metastore_workspaces)}] {workspace.workspace_name}: ERROR - {str(e)[:80]}")
            
            # Calculate share sizes from collected table sizes
            self._calculate_share_sizes()
            
            # Generate summary
            self._log_summary()
            
            # Save results
            self._save_results()
            
            # Export CSV
            self._export_csv()
            
        except Exception as e:
            logger.error(f"Error in collection: {str(e)}", exc_info=True)
            self.result.errors.append({
                'timestamp': datetime.now().isoformat(),
                'level': 'orchestrator',
                'error': str(e)
            })
        
        logger.info("=" * 80)
        logger.info("COLLECTION completed")
        logger.info("=" * 80)
        
        return self.result
    
    def _discover_workspaces(self, target_workspace: str = None) -> List[Workspace]:
        """
        Discover workspaces using Databricks Account API.
        
        Account API provides cloud-specific IDs:
        - Azure: subscription_id, resource_group (from azure_workspace_info)
        - AWS: credentials_id, storage_configuration_id
        - GCP: gcp project info
        """
        logger.info("-" * 80)
        if target_workspace:
            logger.info(f"Step 1: Finding workspace: {target_workspace}")
        else:
            logger.info("Step 1: Discovering Databricks workspaces")
        logger.info("-" * 80)
        
        if not self.account_provider:
            logger.error("Account Provider not available. Set DATABRICKS_ACCOUNT_ID.")
            return []
        
        try:
            logger.info("Using Databricks Account API for workspace discovery...")
            account_workspaces = self.account_provider.list_workspaces()
            workspaces = []
            
            for ws_data in account_workspaces:
                # Skip if filtering and doesn't match
                if target_workspace:
                    ws_name = ws_data['workspace_name']
                    ws_url = ws_data.get('workspace_url', '').rstrip('/')
                    target_clean = target_workspace.rstrip('/')
                    if ws_name != target_workspace and ws_url != target_clean:
                        continue
                
                # Create Workspace with all cloud-specific fields from Account API
                workspace = Workspace(
                    workspace_id=str(ws_data.get('workspace_id', '')),
                    workspace_name=ws_data['workspace_name'],
                    workspace_url=ws_data.get('workspace_url', ''),
                    
                    # Multi-cloud fields
                    cloud=ws_data.get('cloud', 'unknown'),
                    cloud_region=ws_data.get('cloud_region'),
                    workspace_status=ws_data.get('workspace_status'),
                    
                    # Azure-specific (from Account API)
                    subscription_id=ws_data.get('subscription_id'),
                    resource_group=ws_data.get('resource_group'),
                    
                    # AWS-specific
                    aws_account_id=ws_data.get('aws_account_id'),
                    credentials_id=ws_data.get('credentials_id'),
                    storage_configuration_id=ws_data.get('storage_configuration_id'),
                    
                    # GCP-specific
                    gcp_project_id=ws_data.get('gcp_project_id'),
                    
                    # Common
                    location=ws_data.get('cloud_region') or ws_data.get('region', ''),
                    sku=ws_data.get('pricing_tier')
                )
                workspaces.append(workspace)
                
                # Early exit if we found the target
                if target_workspace and workspaces:
                    logger.info(f"Found workspace: {workspace.workspace_name}")
                    break
            
            # Apply exclusions
            exclude_workspaces = self.azure_config.get('exclude_workspaces', [])
            if exclude_workspaces:
                original_count = len(workspaces)
                workspaces = [ws for ws in workspaces if ws.workspace_name not in exclude_workspaces]
                excluded_count = original_count - len(workspaces)
                if excluded_count > 0:
                    logger.info(f"Excluded {excluded_count} workspace(s): {', '.join(exclude_workspaces)}")
            
            logger.info(f"Discovered {len(workspaces)} workspace(s)")
            
            # Log with cloud info
            for ws in workspaces:
                cloud_id = ws.subscription_id or ws.aws_account_id or ws.gcp_project_id or 'N/A'
                logger.info(f"  - {ws.workspace_name} [{ws.cloud}] cloud_id={cloud_id}")
            
            return workspaces
            
        except Exception as e:
            logger.error(f"Error discovering workspaces: {str(e)}")
            self.result.errors.append({
                'timestamp': datetime.now().isoformat(),
                'level': 'cloud_provider',
                'resource': 'workspaces',
                'error': str(e)
            })
            return []
    
    def _get_workspaces_for_metastore(self, metastore_id: str) -> List[Workspace]:
        """
        Get all workspaces assigned to a specific metastore.
        Uses Account API metastore_assignments.list() for efficiency.
        """
        if not self.account_provider:
            logger.warning("Account Provider not available, returning empty list")
            return []
        
        try:
            # Get workspace IDs assigned to this metastore
            assigned_ws_ids = list(self.account_provider.client.metastore_assignments.list(
                metastore_id=metastore_id
            ))
            
            if not assigned_ws_ids:
                logger.warning(f"No workspaces assigned to metastore {metastore_id}")
                return []
            
            # Get full workspace details
            all_workspaces = self.account_provider.list_workspaces()
            ws_id_set = set(int(ws_id) for ws_id in assigned_ws_ids)
            
            # Filter to workspaces in this metastore
            metastore_workspaces = []
            for ws_data in all_workspaces:
                if ws_data['workspace_id'] in ws_id_set:
                    workspace = Workspace(
                        workspace_id=str(ws_data.get('workspace_id', '')),
                        workspace_name=ws_data['workspace_name'],
                        workspace_url=ws_data.get('workspace_url', ''),
                        cloud=ws_data.get('cloud', 'unknown'),
                        cloud_region=ws_data.get('cloud_region'),
                        workspace_status=ws_data.get('workspace_status'),
                        subscription_id=ws_data.get('subscription_id'),
                        resource_group=ws_data.get('resource_group'),
                        aws_account_id=ws_data.get('aws_account_id'),
                        credentials_id=ws_data.get('credentials_id'),
                        storage_configuration_id=ws_data.get('storage_configuration_id'),
                        gcp_project_id=ws_data.get('gcp_project_id'),
                        location=ws_data.get('cloud_region') or ws_data.get('region', ''),
                        sku=ws_data.get('pricing_tier')
                    )
                    metastore_workspaces.append(workspace)
            
            logger.info(f"Found {len(metastore_workspaces)} workspace(s) assigned to metastore")
            return metastore_workspaces
            
        except Exception as e:
            logger.error(f"Error getting workspaces for metastore: {str(e)}")
            return []
    
    def _collect_workspaces_sequential(self, workspaces: List[Workspace]):
        logger.info("-" * 80)
        logger.info("Step 2: Collecting workspace metadata (sequential)")
        logger.info("-" * 80)
        
        for workspace in workspaces:
            self._collect_workspace_metadata(workspace)
    
    def _collect_workspaces_parallel(self, workspaces: List[Workspace], max_workers: int):
        logger.info("-" * 80)
        logger.info(f"Step 2: Collecting workspace metadata (parallel, max_workers={max_workers})")
        logger.info("-" * 80)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_workspace = {
                executor.submit(self._collect_workspace_metadata, ws): ws
                for ws in workspaces
            }
            
            for future in as_completed(future_to_workspace):
                workspace = future_to_workspace[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error collecting workspace {workspace.workspace_name}: {str(e)}")
    
    def _collect_workspace_metadata(self, workspace: Workspace):
        logger.info(f"\nProcessing workspace: {workspace.workspace_name}")
        logger.info("-" * 60)
        
        try:
            # Initialize workspace collector (uses SP credentials from env)
            workspace_collector = WorkspaceCollector(workspace)
            
            # Update workspace with detected type and metastore info
            workspace.workspace_type = workspace_collector.workspace_type
            workspace.has_unity_catalog = (workspace_collector.workspace_type == "UNITY_CATALOG")
            workspace.metastore_id = getattr(workspace_collector, 'metastore_id', None)
            workspace.metastore_name = getattr(workspace_collector, 'metastore_name', None)
            
            # Test connection
            if not workspace_collector.test_connection():
                logger.error(f"Failed to connect to workspace {workspace.workspace_name}")
                self.result.errors.append({
                    'timestamp': datetime.now().isoformat(),
                    'level': 'workspace',
                    'workspace': workspace.workspace_name,
                    'error': 'Connection failed'
                })
                return
            
            # Collect catalogs (Workspace Level)
            catalogs = workspace_collector.list_catalogs()
            self.result.catalogs.extend(catalogs)
            
            # Collect catalog bindings (which workspaces can access each catalog)
            # Filter out excluded catalogs before checking bindings
            if catalogs:
                exclude_catalogs = self.azure_config.get('exclude_catalogs', [])
                catalogs_to_check = [c for c in catalogs if c.catalog_name not in exclude_catalogs]
                
                if catalogs_to_check:
                    # Get metastore info from first catalog
                    metastore_id = catalogs_to_check[0].metastore_id if catalogs_to_check[0].metastore_id else None
                    metastore_name = catalogs_to_check[0].metastore_name if catalogs_to_check[0].metastore_name else None
                    if metastore_id:
                        bindings = workspace_collector.list_catalog_bindings(catalogs_to_check, metastore_id, metastore_name)
                        self.result.catalog_bindings.extend(bindings)
                        logger.info(f"  - Catalog Bindings: {len(bindings)}")
            
            # Collect external locations
            external_locations = workspace_collector.list_external_locations()
            self.result.external_locations.extend(external_locations)
            logger.info(f"  - External Locations: {len(external_locations)}")
            
            # Collect Delta shares
            shares = workspace_collector.list_shares()
            self.result.shares.extend(shares)
            logger.info(f"  - Delta Shares: {len(shares)}")
            
            # Collect Git repos
            repos = workspace_collector.list_repos()
            self.result.repos.extend(repos)
            logger.info(f"  - Git Repos: {len(repos)}")
            
            # Collect workspace configurations
            workspace_configs = workspace_collector.list_workspace_configs()
            self.result.workspace_configs.extend(workspace_configs)
            logger.info(f"  - Workspace Configs: {len(workspace_configs)}")
            
            # Collect admin users and groups
            admin_info = workspace_collector.get_workspace_admins()
            workspace.admin_user_count = admin_info.get('admin_user_count', 0)
            workspace.admin_group_count = admin_info.get('admin_group_count', 0)
            workspace.admin_users = admin_info.get('admin_users', [])
            workspace.admin_groups = admin_info.get('admin_groups', [])
            logger.info(f"  - Admin Users: {workspace.admin_user_count}, Admin Groups: {workspace.admin_group_count}")
            
            # Collect IP access list info
            ip_info = workspace_collector.get_ip_access_lists()
            workspace.ip_access_list_enabled = ip_info.get('ip_access_list_enabled', False)
            workspace.ip_access_list_count = ip_info.get('ip_access_list_count', 0)
            logger.info(f"  - IP Access Lists: enabled={workspace.ip_access_list_enabled}, count={workspace.ip_access_list_count}")
            
            # Collect private endpoint info (uses Account API NCC automatically)
            pe_info = workspace_collector.get_private_endpoint_info()
            workspace.private_access_enabled = pe_info.get('private_access_enabled', False)
            workspace.serverless_private_endpoint_count = pe_info.get('serverless_private_endpoint_count', 0)
            logger.info(f"  - Private Endpoints: {workspace.serverless_private_endpoint_count}, Private Access: {workspace.private_access_enabled}")
            
            # Collect workspace features (dynamic - all accessible settings)
            features = workspace_collector.get_workspace_features()
            workspace.workspace_features = features  # Store all features
            # Legacy fields for CSV compatibility
            workspace.serverless_compute_enabled = features.get('serverless_compute_enabled', False)
            workspace.model_serving_enabled = features.get('model_serving_enabled', False)
            workspace.tokens_enabled = features.get('enableTokensConfig', False)
            workspace.max_token_lifetime_days = features.get('maxTokenLifetimeDays')
            workspace.results_downloading_enabled = features.get('enableResultsDownloading', False)
            workspace.web_terminal_enabled = features.get('enableWebTerminal', False)
            workspace.user_isolation_enabled = features.get('enforceUserIsolation', False)
            
            if len(catalogs) == 0:
                logger.info(f"No catalogs found (Unity Catalog may not be enabled)")
            else:
                logger.info(f"Found {len(catalogs)} catalog(s)")
            
            # Collect catalog-level metadata
            exclude_catalogs = self.azure_config.get('exclude_catalogs', [])
            for catalog in catalogs:
                # Check if catalog should be excluded
                if catalog.catalog_name in exclude_catalogs:
                    logger.info(f"  Skipping catalog: {catalog.catalog_name} (excluded)")
                    continue
                
                # Skip detailed collection for legacy HMS catalogs (just metadata entry)
                if catalog.is_legacy_hms:
                    logger.info(f"  Skipping detailed collection for legacy HMS catalog: {catalog.catalog_name}")
                    continue
                
                self._collect_catalog_metadata(catalog, workspace_collector.client, workspace.workspace_url)
        
        except Exception as e:
            logger.error(f"Error processing workspace {workspace.workspace_name}: {str(e)}", exc_info=True)
            self.result.errors.append({
                'timestamp': datetime.now().isoformat(),
                'level': 'workspace',
                'workspace': workspace.workspace_name,
                'error': str(e)
            })
    
    def _collect_catalog_metadata(self, catalog: Catalog, client, workspace_url: str):
        logger.info(f"  Processing catalog: {catalog.catalog_name}")
        
        try:
            # Initialize catalog collector
            exclude_schemas = self.azure_config.get('exclude_schemas', [])
            
            catalog_collector = CatalogCollector(
                catalog, 
                client, 
                workspace_url, 
                include_column_details=self.config['collection'].get('include_column_details', True),
                exclude_schemas=exclude_schemas
            )
            
            # Collect schemas
            schemas = catalog_collector.list_schemas()
            self.result.schemas.extend(schemas)
            logger.info(f"    - Schemas: {len(schemas)}")
            
            # Collect tables if enabled
            if self.collection_config.get('collect_tables', True):
                warehouse_id = self.collection_config.get('warehouse_id')
                collect_sizes = self.collection_config.get('collect_sizes', False)
                
                # Only pass warehouse_id if size collection is enabled
                wh_id = warehouse_id if collect_sizes else None
                tables = catalog_collector.list_all_tables(warehouse_id=wh_id)
                self.result.tables.extend(tables)
                
                # Log table types
                table_types = {}
                for table in tables:
                    table_type = table.table_type
                    table_types[table_type] = table_types.get(table_type, 0) + 1
                
                logger.info(f"    - Tables: {len(tables)}")
                for table_type, count in table_types.items():
                    logger.info(f"      * {table_type}: {count}")
            
            # Collect volumes if enabled
            if self.collection_config.get('collect_volumes', True):
                volumes = catalog_collector.list_all_volumes()
                self.result.volumes.extend(volumes)
                logger.info(f"    - Volumes: {len(volumes)}")
        
        except Exception as e:
            logger.error(f"Error processing catalog {catalog.catalog_name}: {str(e)}")
            self.result.errors.append({
                'timestamp': datetime.now().isoformat(),
                'level': 'catalog',
                'catalog': catalog.catalog_name,
                'error': str(e)
            })
    
    def _log_summary(self):
        logger.info("\n" + "=" * 80)
        logger.info("COLLECTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Subscription ID: {self.result.subscription_id}")
        logger.info(f"Collection Time: {self.result.collection_timestamp}")
        logger.info("-" * 80)
        logger.info(f"Workspaces:      {len(self.result.workspaces)}")
        logger.info(f"Catalogs:        {len(self.result.catalogs)}")
        logger.info(f"Schemas:         {len(self.result.schemas)}")
        logger.info(f"Tables/Views:    {len(self.result.tables)}")
        logger.info(f"Volumes:         {len(self.result.volumes)}")
        logger.info(f"Errors:          {len(self.result.errors)}")
        
        # Workspace grouping by metastore
        if self.result.workspaces:
            logger.info("-" * 80)
            logger.info("WORKSPACE GROUPING:")
            
            # Group by metastore
            metastore_groups = {}
            hms_workspaces = []
            region_groups = {}
            
            for ws in self.result.workspaces:
                # Group by region
                region = ws.location or "unknown"
                if region not in region_groups:
                    region_groups[region] = []
                region_groups[region].append(ws.workspace_name)
                
                # Group by metastore
                if ws.has_unity_catalog and ws.metastore_id:
                    metastore_key = ws.metastore_name or ws.metastore_id
                    if metastore_key not in metastore_groups:
                        metastore_groups[metastore_key] = []
                    metastore_groups[metastore_key].append(ws.workspace_name)
                elif not ws.has_unity_catalog:
                    hms_workspaces.append(ws.workspace_name)
            
            # Log metastore groups
            logger.info("")
            logger.info("  By Metastore:")
            for metastore, workspaces in metastore_groups.items():
                logger.info(f"    [{metastore}]: {len(workspaces)} workspace(s)")
                for ws_name in workspaces:
                    logger.info(f"      - {ws_name}")
            
            if hms_workspaces:
                logger.info(f"    [HIVE_METASTORE (no UC)]: {len(hms_workspaces)} workspace(s)")
                for ws_name in hms_workspaces:
                    logger.info(f"      - {ws_name}")
            
            # Log region groups
            logger.info("")
            logger.info("  By Region:")
            for region, workspaces in region_groups.items():
                logger.info(f"    [{region}]: {len(workspaces)} workspace(s)")
                for ws_name in workspaces:
                    logger.info(f"      - {ws_name}")
            
            # Security & Access Summary
            logger.info("")
            logger.info("-" * 80)
            logger.info("SECURITY & ACCESS SUMMARY:")
            
            total_admin_users = sum(ws.admin_user_count for ws in self.result.workspaces)
            total_admin_groups = sum(ws.admin_group_count for ws in self.result.workspaces)
            workspaces_with_ip_lists = [ws for ws in self.result.workspaces if ws.ip_access_list_enabled]
            workspaces_with_private_access = [ws for ws in self.result.workspaces if ws.private_access_enabled]
            total_private_endpoints = sum(ws.serverless_private_endpoint_count for ws in self.result.workspaces)
            
            logger.info(f"  Total Admin Users (across all workspaces): {total_admin_users}")
            logger.info(f"  Total Admin Groups (across all workspaces): {total_admin_groups}")
            logger.info("")
            logger.info(f"  Workspaces with IP Access Lists Enabled: {len(workspaces_with_ip_lists)}")
            for ws in workspaces_with_ip_lists:
                logger.info(f"    - {ws.workspace_name} ({ws.ip_access_list_count} list(s))")
            
            logger.info(f"  Workspaces with Private Access Enabled: {len(workspaces_with_private_access)}")
            for ws in workspaces_with_private_access:
                logger.info(f"    - {ws.workspace_name}")
            
            logger.info(f"  Total Serverless Private Endpoints: {total_private_endpoints}")
        
        # Table type breakdown
        if self.result.tables:
            logger.info("-" * 80)
            logger.info("Table Types:")
            table_types = {}
            formats = {}
            total_size = 0
            uniform_count = 0
            iceberg_count = 0
            delta_count = 0
            
            for table in self.result.tables:
                table_types[table.table_type] = table_types.get(table.table_type, 0) + 1
                
                if table.table_format:
                    formats[table.table_format] = formats.get(table.table_format, 0) + 1
                
                if table.is_uniform:
                    uniform_count += 1
                if table.is_iceberg:
                    iceberg_count += 1
                if table.is_delta:
                    delta_count += 1
                
                if table.size_bytes:
                    total_size += table.size_bytes
            
            for table_type, count in sorted(table_types.items()):
                logger.info(f"  {table_type}: {count}")
            
            logger.info("-" * 80)
            logger.info("Table Formats:")
            for format_type, count in sorted(formats.items()):
                logger.info(f"  {format_type}: {count}")
            
            logger.info("-" * 80)
            logger.info("Special Table Types:")
            logger.info(f"  Delta Tables: {delta_count}")
            logger.info(f"  Iceberg Tables: {iceberg_count}")
            logger.info(f"  UniForm Tables: {uniform_count}")
            
            if total_size > 0:
                logger.info("-" * 80)
                logger.info(f"Total Table Size: {format_bytes(total_size)}")
        
        # Delta Shares summary
        if self.result.shares:
            logger.info("-" * 80)
            logger.info("DELTA SHARES:")
            total_share_size = 0
            total_shared_tables = 0
            total_recipients = 0
            
            for share in self.result.shares:
                size_str = format_bytes(share.total_size_bytes) if share.total_size_bytes else "N/A"
                recipients_str = ', '.join(share.recipients[:3]) if share.recipients else "None"
                if len(share.recipients) > 3:
                    recipients_str += f", +{len(share.recipients) - 3} more"
                
                logger.info(f"  {share.share_name}:")
                logger.info(f"    Tables: {share.table_count}, Schemas: {share.schema_count}, Size: {size_str}")
                logger.info(f"    Recipients ({share.recipient_count}): {recipients_str}")
                
                if share.total_size_bytes:
                    total_share_size += share.total_size_bytes
                total_shared_tables += share.table_count
                total_recipients += share.recipient_count
            
            logger.info("")
            logger.info(f"  Total Shares: {len(self.result.shares)}")
            logger.info(f"  Total Shared Tables: {total_shared_tables}")
            logger.info(f"  Total Recipients: {total_recipients}")
            if total_share_size > 0:
                logger.info(f"  Total Shared Data Size: {format_bytes(total_share_size)}")
        
        logger.info("=" * 80)
    
    def _save_results(self):
        """Save collection results - CSV export only (no JSON)"""
        # CSV export is handled by _export_csv, called separately
        # This method is now a no-op as we only use CSV
        pass
    
    def _calculate_share_sizes(self):
        """Calculate Delta Share sizes from collected table sizes"""
        if not self.result.shares or not self.result.tables:
            return
        
        # Build a lookup map: full_table_name -> size_bytes
        table_size_map = {}
        for table in self.result.tables:
            full_name = f"{table.catalog_name}.{table.schema_name}.{table.table_name}"
            if table.size_bytes:
                table_size_map[full_name] = table.size_bytes
        
        # Calculate size for each share
        for share in self.result.shares:
            if share.shared_tables:
                total_size = 0
                for table_name in share.shared_tables:
                    if table_name in table_size_map:
                        total_size += table_size_map[table_name]
                
                if total_size > 0:
                    share.total_size_bytes = total_size
                    logger.debug(f"Share {share.share_name}: {len(share.shared_tables)} tables, size={format_bytes(total_size)}")
    
    def _export_csv(self):
        output_dir = self.output_config.get('directory', './output')
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            result_dict = self.result.to_dict()
            
            # Check if tables were already saved per catalog (incremental mode)
            tables_already_saved = hasattr(self, '_catalog_table_files') and self._catalog_table_files
            if tables_already_saved:
                # Clear tables from result_dict to skip re-exporting
                # Tables are in separate per-catalog files
                result_dict['tables'] = []
                logger.info(f"  Tables already saved to {len(self._catalog_table_files)} per-catalog files ({self._total_tables_saved} tables)")
            
            # Export collection results (scan data already exported via _save_scan_results)
            export_collection_to_csv(result_dict, output_dir, timestamp)
            logger.info(f"CSV files exported to: {output_dir}")
        
        except Exception as e:
            logger.error(f"Error exporting CSV: {str(e)}")