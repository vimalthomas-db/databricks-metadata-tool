"""
Databricks Account API Provider - Multi-Cloud Edition

Supports OAuth M2M (Databricks native) and Azure SP authentication.
Extracts cloud-specific IDs (subscription_id, AWS account, GCP project) from Account API.

Authentication Priority:
1. OAuth M2M (DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET) - Multi-cloud
2. Azure SP (AZURE_CLIENT_ID + AZURE_TENANT_ID + AZURE_CLIENT_SECRET) - Azure only
"""

import logging
import os
from typing import List, Dict, Optional, Any
from databricks.sdk import AccountClient

from ..models import Workspace, MetastoreInfo

logger = logging.getLogger('databricks_metadata_tool.account_provider')


# Cloud-specific Account API hosts
ACCOUNT_HOSTS = {
    'azure': 'https://accounts.azuredatabricks.net',
    'aws': 'https://accounts.cloud.databricks.com',
    'gcp': 'https://accounts.gcp.databricks.com',
}


class AccountProvider:
    """
    Multi-cloud Databricks Account API provider.
    
    Supports:
    - OAuth M2M authentication (recommended for multi-cloud)
    - Azure Service Principal authentication (backward compatible)
    - Cloud-specific ID extraction from Account API
    """
    
    def __init__(self, account_id: str, account_host: str = None, cloud: str = 'azure'):
        self.account_id = account_id
        self.cloud = cloud.lower()
        self.account_host = account_host or ACCOUNT_HOSTS.get(self.cloud, ACCOUNT_HOSTS['azure'])
        
        # Determine authentication method
        self.auth_method = self._get_auth_method()
        
        # Initialize Account Client
        self.client = self._create_client()
        
        logger.info(f"Account Provider initialized")
        logger.info(f"  Account ID: {account_id}")
        logger.info(f"  Host: {self.account_host}")
        logger.info(f"  Auth: {self.auth_method}")
    
    def _get_auth_method(self) -> str:
        """Determine which authentication method to use"""
        # Priority 1: OAuth M2M (Databricks native - multi-cloud)
        if os.getenv('DATABRICKS_CLIENT_ID') and os.getenv('DATABRICKS_CLIENT_SECRET'):
            return 'oauth_m2m'
        
        # Priority 2: Azure Service Principal (backward compatible)
        if os.getenv('AZURE_CLIENT_ID') and os.getenv('AZURE_CLIENT_SECRET'):
            return 'azure_sp'
        
        raise ValueError(
            "No valid credentials found. Set one of:\n"
            "  1. DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (OAuth M2M - recommended)\n"
            "  2. AZURE_CLIENT_ID + AZURE_TENANT_ID + AZURE_CLIENT_SECRET (Azure SP)"
        )
    
    def _create_client(self) -> AccountClient:
        """Create AccountClient with appropriate authentication"""
        if self.auth_method == 'oauth_m2m':
            return AccountClient(
                host=self.account_host,
                account_id=self.account_id,
                client_id=os.getenv('DATABRICKS_CLIENT_ID'),
                client_secret=os.getenv('DATABRICKS_CLIENT_SECRET')
            )
        else:  # azure_sp
            return AccountClient(
                host=self.account_host,
                account_id=self.account_id,
                azure_client_id=os.getenv('AZURE_CLIENT_ID'),
                azure_tenant_id=os.getenv('AZURE_TENANT_ID'),
                azure_client_secret=os.getenv('AZURE_CLIENT_SECRET')
            )
    
    def _create_workspace_client(self, workspace_url: str):
        """Create WorkspaceClient with same auth method"""
        from databricks.sdk import WorkspaceClient
        
        if self.auth_method == 'oauth_m2m':
            return WorkspaceClient(
                host=workspace_url,
                client_id=os.getenv('DATABRICKS_CLIENT_ID'),
                client_secret=os.getenv('DATABRICKS_CLIENT_SECRET')
            )
        else:  # azure_sp
            return WorkspaceClient(
                host=workspace_url,
                azure_client_id=os.getenv('AZURE_CLIENT_ID'),
                azure_tenant_id=os.getenv('AZURE_TENANT_ID'),
                azure_client_secret=os.getenv('AZURE_CLIENT_SECRET')
            )
    
    # =========================================================================
    # Cloud Info Extraction
    # =========================================================================
    
    def _extract_cloud_info(self, ws) -> Dict[str, Any]:
        """
        Extract cloud-specific IDs from workspace object.
        Account API returns different fields per cloud.
        """
        cloud = 'unknown'
        if hasattr(ws, 'cloud') and ws.cloud:
            cloud = str(ws.cloud).lower()
        elif hasattr(ws, 'deployment_name') and ws.deployment_name:
            # Infer from deployment URL
            if 'azuredatabricks' in str(getattr(ws, 'deployment_name', '')).lower():
                cloud = 'azure'
        
        info = {
            'cloud': cloud,
            'cloud_region': None,
            'subscription_id': None,
            'resource_group': None,
            'aws_account_id': None,
            'credentials_id': None,
            'storage_configuration_id': None,
            'gcp_project_id': None,
        }
        
        # Azure-specific extraction
        if cloud == 'azure':
            info['cloud_region'] = getattr(ws, 'location', None)
            
            # azure_workspace_info contains subscription_id and resource_group
            azure_info = getattr(ws, 'azure_workspace_info', None)
            if azure_info:
                info['subscription_id'] = getattr(azure_info, 'subscription_id', None)
                info['resource_group'] = getattr(azure_info, 'resource_group', None)
            
            # Build workspace URL
            deployment = getattr(ws, 'deployment_name', None)
            info['workspace_url'] = f"https://{deployment}.azuredatabricks.net" if deployment else None
        
        # AWS-specific extraction
        elif cloud == 'aws':
            info['cloud_region'] = getattr(ws, 'aws_region', None)
            info['credentials_id'] = getattr(ws, 'credentials_id', None)
            info['storage_configuration_id'] = getattr(ws, 'storage_configuration_id', None)
            
            # AWS account ID may be derivable from credentials_id or network config
            # For now, store credentials_id as reference
            if info['credentials_id']:
                info['aws_account_id'] = info['credentials_id']  # Placeholder - needs lookup
            
            # Build workspace URL
            deployment = getattr(ws, 'deployment_name', None)
            info['workspace_url'] = f"https://{deployment}.cloud.databricks.com" if deployment else None
        
        # GCP-specific extraction
        elif cloud == 'gcp':
            info['cloud_region'] = getattr(ws, 'location', None)
            
            # GCP workspace info
            gcp_info = getattr(ws, 'gcp_managed_network_config', None)
            if gcp_info:
                # GCP project may be in network config
                pass
            
            # Build workspace URL
            deployment = getattr(ws, 'deployment_name', None)
            info['workspace_url'] = f"https://{deployment}.gcp.databricks.com" if deployment else None
        
        return info
    
    # =========================================================================
    # Core List Methods
    # =========================================================================
    
    def list_workspaces(self) -> List[Dict[str, Any]]:
        """List all workspaces with cloud-specific info extraction"""
        logger.info("Discovering workspaces via Account API...")
        
        workspaces = []
        
        try:
            workspace_list = list(self.client.workspaces.list())
            
            for ws in workspace_list:
                # Extract cloud-specific info
                cloud_info = self._extract_cloud_info(ws)
                
                workspace = {
                    'workspace_id': ws.workspace_id,
                    'workspace_name': ws.workspace_name,
                    'deployment_name': ws.deployment_name,
                    'workspace_url': cloud_info.get('workspace_url'),
                    'workspace_status': ws.workspace_status.value if hasattr(ws, 'workspace_status') and ws.workspace_status else None,
                    'pricing_tier': ws.pricing_tier.value if hasattr(ws, 'pricing_tier') and ws.pricing_tier else None,
                    
                    # Multi-cloud fields
                    'cloud': cloud_info['cloud'],
                    'cloud_region': cloud_info['cloud_region'],
                    
                    # Azure fields
                    'subscription_id': cloud_info['subscription_id'],
                    'resource_group': cloud_info['resource_group'],
                    
                    # AWS fields
                    'aws_account_id': cloud_info['aws_account_id'],
                    'credentials_id': cloud_info['credentials_id'],
                    'storage_configuration_id': cloud_info['storage_configuration_id'],
                    
                    # GCP fields
                    'gcp_project_id': cloud_info['gcp_project_id'],
                    
                    # Legacy compatibility
                    'region': cloud_info['cloud_region'],
                }
                workspaces.append(workspace)
                
                cloud_id = cloud_info.get('subscription_id') or cloud_info.get('aws_account_id') or cloud_info.get('gcp_project_id') or 'N/A'
                logger.debug(f"  {ws.workspace_name} [{cloud_info['cloud']}] cloud_id={cloud_id}")
        
        except Exception as e:
            logger.error(f"Error listing workspaces from Account API: {str(e)}")
            raise
        
        # Log cloud distribution
        cloud_counts = {}
        for ws in workspaces:
            c = ws.get('cloud', 'unknown')
            cloud_counts[c] = cloud_counts.get(c, 0) + 1
        
        logger.info(f"Discovered {len(workspaces)} workspace(s)")
        for cloud, count in cloud_counts.items():
            logger.info(f"  {cloud.upper()}: {count}")
        
        return workspaces
    
    def list_metastores(self) -> List[MetastoreInfo]:
        """List all metastores via Account API"""
        logger.info("Discovering metastores via Account API...")
        
        metastores = []
        
        try:
            metastore_list = list(self.client.metastores.list())
            
            for ms in metastore_list:
                metastore = MetastoreInfo(
                    metastore_id=ms.metastore_id,
                    metastore_name=ms.name if hasattr(ms, 'name') else None,
                    region=ms.region if hasattr(ms, 'region') else None,
                )
                metastores.append(metastore)
                logger.debug(f"  {ms.name} ({ms.metastore_id}) in {ms.region}")
        
        except Exception as e:
            logger.error(f"Error listing metastores: {str(e)}")
            raise
        
        logger.info(f"Discovered {len(metastores)} metastore(s)")
        return metastores
    
    def list_network_connectivity_configs(self) -> List[Dict[str, Any]]:
        """List Network Connectivity Configurations (NCC)"""
        logger.info("Getting network connectivity configurations...")
        
        nccs = []
        try:
            ncc_list = list(self.client.network_connectivity.list_network_connectivity_configurations())
            
            for ncc in ncc_list:
                ncc_info = {
                    'ncc_id': ncc.network_connectivity_config_id,
                    'name': ncc.name,
                    'region': ncc.region,
                    'private_endpoint_count': 0,
                    'private_endpoints': []
                }
                
                if ncc.egress_config and ncc.egress_config.target_rules:
                    rules = ncc.egress_config.target_rules
                    if rules.azure_private_endpoint_rules:
                        for rule in rules.azure_private_endpoint_rules:
                            ncc_info['private_endpoint_count'] += 1
                            ncc_info['private_endpoints'].append({
                                'rule_id': rule.rule_id,
                                'target_resource': rule.resource_id.split('/')[-1] if rule.resource_id else 'unknown',
                                'resource_type': rule.group_id or 'unknown',
                                'status': str(rule.connection_state) if rule.connection_state else 'Unknown'
                            })
                
                nccs.append(ncc_info)
                logger.debug(f"  {ncc.name} ({ncc.region}) - {ncc_info['private_endpoint_count']} PE rules")
        
        except Exception as e:
            logger.warning(f"Could not list NCCs: {str(e)}")
        
        logger.info(f"Found {len(nccs)} network connectivity configuration(s)")
        return nccs
    
    # =========================================================================
    # Quick Scan (Account API Only)
    # =========================================================================
    
    def quick_scan(self, test_workspace: str = None) -> Dict[str, Any]:
        """
        Quick scan using ONLY Account API calls.
        No workspace connections - fastest mode (~5-10s for 100 workspaces).
        """
        logger.info("=" * 80)
        logger.info(f"ACCOUNT SCAN [QUICK MODE] - Auth: {self.auth_method}")
        logger.info("=" * 80)
        
        result = self._init_scan_result('quick')
        
        # List all workspaces with cloud info
        all_workspaces = self.list_workspaces()
        
        if test_workspace:
            workspaces = [ws for ws in all_workspaces if ws['workspace_name'] == test_workspace]
            if not workspaces:
                logger.error(f"Workspace '{test_workspace}' not found")
                available = ', '.join([ws['workspace_name'] for ws in all_workspaces[:10]])
                logger.info(f"Available: {available}")
                return result
        else:
            workspaces = all_workspaces
        
        result['workspaces'] = workspaces
        workspace_id_map = {ws['workspace_id']: ws for ws in workspaces}
        
        # List metastores
        metastores = self.list_metastores()
        result['metastores'] = [m.to_dict() for m in metastores]
        
        # Get metastore assignments (parallel)
        logger.info("Checking metastore assignments...")
        assignments = {ms.metastore_id: [] for ms in metastores}
        assigned_workspace_ids = set()
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def get_metastore_assignments(ms):
            try:
                ws_ids = list(self.client.metastore_assignments.list(metastore_id=ms.metastore_id))
                return ms.metastore_id, [int(ws_id) for ws_id in ws_ids]
            except Exception as e:
                logger.debug(f"  Error getting assignments for {ms.metastore_name}: {str(e)[:50]}")
                return ms.metastore_id, []
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(get_metastore_assignments, ms) for ms in metastores]
            for future in as_completed(futures):
                ms_id, ws_ids = future.result()
                assignments[ms_id] = ws_ids
                assigned_workspace_ids.update(ws_ids)
        
        # Classify workspaces
        for ws in workspaces:
            ws_name = ws['workspace_name']
            ws_id = ws['workspace_id']
            pricing_tier = (ws.get('pricing_tier') or '').upper()
            
            if pricing_tier == 'STANDARD':
                result['hms_workspaces'].append(ws_name)
            elif ws_id in assigned_workspace_ids:
                result['uc_workspaces'].append(ws_name)
            else:
                result['hms_workspaces'].append(ws_name)
        
        # Build metastore map
        self._build_metastore_map(result, assignments, metastores, workspace_id_map)
        
        # Get NCC info
        result['network_connectivity'] = self.list_network_connectivity_configs()
        
        self._log_scan_summary(result, metastores)
        return result
    
    # =========================================================================
    # Deep Scan (Account API + Workspace Connections)
    # =========================================================================
    
    def deep_scan(self, test_workspace: str = None) -> Dict[str, Any]:
        """
        Deep scan that verifies by connecting to each workspace.
        Slower but verifies accessibility (~30-60s for 100 workspaces).
        """
        logger.info("=" * 80)
        logger.info(f"ACCOUNT SCAN [DEEP MODE] - Auth: {self.auth_method}")
        logger.info("=" * 80)
        
        result = self._init_scan_result('deep')
        
        all_workspaces = self.list_workspaces()
        
        if test_workspace:
            workspaces = [ws for ws in all_workspaces if ws['workspace_name'] == test_workspace]
            if not workspaces:
                logger.error(f"Workspace '{test_workspace}' not found")
                return result
        else:
            workspaces = all_workspaces
        
        result['workspaces'] = workspaces
        workspace_id_map = {ws['workspace_id']: ws for ws in workspaces}
        
        metastores = self.list_metastores()
        result['metastores'] = [m.to_dict() for m in metastores]
        
        # Verify via workspace connections
        logger.info("Verifying via workspace connections...")
        assignments, errors, inaccessible = self._verify_via_workspace_connections(workspaces, metastores)
        
        result['errors'].extend(errors)
        result['inaccessible_workspaces'] = inaccessible
        
        inaccessible_set = set(inaccessible)
        assigned_ids = set()
        for ws_ids in assignments.values():
            assigned_ids.update(ws_ids)
        
        for ws in workspaces:
            if ws['workspace_id'] in assigned_ids:
                result['uc_workspaces'].append(ws['workspace_name'])
            elif ws['workspace_name'] not in inaccessible_set:
                result['hms_workspaces'].append(ws['workspace_name'])
        
        self._build_metastore_map(result, assignments, metastores, workspace_id_map)
        result['network_connectivity'] = self.list_network_connectivity_configs()
        
        self._log_scan_summary(result, metastores)
        return result
    
    def _verify_via_workspace_connections(self, workspaces: List[Dict], metastores: List) -> tuple:
        """Connect to each workspace to verify metastore assignment"""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading
        
        assignments = {ms.metastore_id: [] for ms in metastores}
        errors = []
        inaccessible = []
        lock = threading.Lock()
        
        def check_workspace(ws):
            ws_url = ws.get('workspace_url')
            ws_name = ws['workspace_name']
            pricing_tier = (ws.get('pricing_tier') or '').upper()
            
            if pricing_tier == 'STANDARD':
                return {'status': 'hms', 'workspace_name': ws_name, 'workspace_id': ws['workspace_id']}
            
            if not ws_url:
                return {'status': 'no_url', 'workspace_name': ws_name}
            
            try:
                client = self._create_workspace_client(ws_url)
                current = client.metastores.current()
                ms_id = getattr(current, 'metastore_id', None)
                
                if ms_id:
                    return {
                        'status': 'uc',
                        'workspace_id': ws['workspace_id'],
                        'workspace_name': ws_name,
                        'metastore_id': ms_id
                    }
                else:
                    return {'status': 'hms', 'workspace_name': ws_name, 'workspace_id': ws['workspace_id']}
                    
            except Exception as e:
                error_msg = str(e).lower()
                
                if any(x in error_msg for x in ['not assigned', 'no metastore', 'standard_tier']):
                    return {'status': 'hms', 'workspace_name': ws_name, 'workspace_id': ws['workspace_id']}
                
                if any(x in error_msg for x in ['unauthorized', '403', 'blocked', 'timed out', 'timeout']):
                    return {'status': 'inaccessible', 'workspace_name': ws_name, 'error': str(e)[:100]}
                
                return {'status': 'error', 'workspace_name': ws_name, 'error': str(e)[:100]}
        
        max_workers = min(20, len(workspaces))
        completed = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(check_workspace, ws): ws for ws in workspaces}
            
            for future in as_completed(futures):
                completed += 1
                result = future.result()
                
                if result:
                    status = result.get('status')
                    
                    if status == 'uc':
                        ms_id = result['metastore_id']
                        with lock:
                            if ms_id in assignments:
                                assignments[ms_id].append(result['workspace_id'])
                            else:
                                assignments[ms_id] = [result['workspace_id']]
                    elif status in ('inaccessible', 'error'):
                        with lock:
                            inaccessible.append(result['workspace_name'])
                            errors.append({
                                'workspace': result['workspace_name'],
                                'error': 'access_denied',
                                'message': result.get('error', '')
                            })
                
                if completed % 10 == 0 or completed == len(workspaces):
                    logger.info(f"  Progress: {completed}/{len(workspaces)}")
        
        return assignments, errors, inaccessible
    
    # =========================================================================
    # Helper Methods
    # =========================================================================
    
    def _init_scan_result(self, mode: str) -> Dict[str, Any]:
        return {
            'account_id': self.account_id,
            'scan_mode': mode,
            'auth_method': self.auth_method,
            'workspaces': [],
            'metastores': [],
            'uc_workspaces': [],
            'hms_workspaces': [],
            'inaccessible_workspaces': [],
            'metastore_workspace_map': {},
            'network_connectivity': [],
            'errors': [],
        }
    
    def _build_metastore_map(self, result: Dict, assignments: Dict, metastores: List, workspace_id_map: Dict):
        for metastore_id, ws_ids in assignments.items():
            metastore_name = next((m.metastore_name for m in metastores if m.metastore_id == metastore_id), metastore_id)
            
            result['metastore_workspace_map'][metastore_id] = {
                'metastore_name': metastore_name,
                'workspace_ids': ws_ids,
                'workspace_names': [workspace_id_map[wid]['workspace_name'] for wid in ws_ids if wid in workspace_id_map]
            }
            
            for ms in metastores:
                if ms.metastore_id == metastore_id:
                    ms.workspace_count = len(ws_ids)
                    ms.workspaces = [workspace_id_map[wid]['workspace_name'] for wid in ws_ids if wid in workspace_id_map]
                    if ms.workspaces:
                        ms.suggested_admin_workspace = ms.workspaces[0]
    
    def _log_scan_summary(self, result: Dict, metastores: List):
        mode = result['scan_mode'].upper()
        logger.info("")
        logger.info("-" * 80)
        logger.info(f"SCAN SUMMARY [{mode}]")
        logger.info("-" * 80)
        logger.info(f"Total Workspaces:       {len(result['workspaces'])}")
        logger.info(f"Total Metastores:       {len(metastores)}")
        logger.info(f"UC Workspaces:          {len(result['uc_workspaces'])}")
        logger.info(f"HMS-Only Workspaces:    {len(result['hms_workspaces'])}")
        logger.info(f"Inaccessible:           {len(result['inaccessible_workspaces'])}")
        logger.info(f"Network Configs (NCC):  {len(result['network_connectivity'])}")
        
        # Cloud distribution
        cloud_counts = {}
        for ws in result['workspaces']:
            c = ws.get('cloud', 'unknown')
            cloud_counts[c] = cloud_counts.get(c, 0) + 1
        if cloud_counts:
            logger.info(f"By Cloud: {cloud_counts}")
        
        logger.info("")
        logger.info("METASTORES:")
        for ms in metastores:
            logger.info(f"  [{ms.region}] {ms.metastore_name}")
            if ms.workspaces:
                ws_list = ', '.join(ms.workspaces[:5])
                if ms.workspace_count > 5:
                    ws_list += f" (+{ms.workspace_count - 5} more)"
                logger.info(f"    Workspaces ({ms.workspace_count}): {ws_list}")
        
        if result['hms_workspaces']:
            logger.info("")
            logger.info(f"HMS-ONLY ({len(result['hms_workspaces'])}):")
            for ws_name in result['hms_workspaces'][:5]:
                logger.info(f"  - {ws_name}")
            if len(result['hms_workspaces']) > 5:
                logger.info(f"  ... and {len(result['hms_workspaces']) - 5} more")
    
    # =========================================================================
    # Legacy Compatibility
    # =========================================================================
    
    def scan_account(self, test_workspace: str = None, deep_scan: bool = False) -> Dict[str, Any]:
        """Wrapper for quick_scan/deep_scan"""
        if deep_scan:
            return self.deep_scan(test_workspace)
        return self.quick_scan(test_workspace)
    
    def test_connection(self) -> bool:
        """Test Account API connection"""
        try:
            list(self.client.workspaces.list())
            logger.info(f"Account API connection successful (auth={self.auth_method})")
            return True
        except Exception as e:
            logger.error(f"Account API connection failed: {str(e)}")
            return False
