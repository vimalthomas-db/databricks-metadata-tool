"""Export scan and collection results to CSV files"""

import csv
import os
import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger('databricks_metadata_tool.scan_exporter')


def export_catalog_tables_csv(tables: List[Dict[str, Any]], catalog_name: str, 
                               output_dir: str, timestamp: str, collection_ts: str) -> str:
    """Export tables for a single catalog to CSV (incremental save)."""
    os.makedirs(output_dir, exist_ok=True)
    safe_catalog_name = catalog_name.replace('/', '_').replace('\\', '_').replace(' ', '_')
    tables_file = os.path.join(output_dir, f'collect_tables_{safe_catalog_name}_{timestamp}.csv')
    
    with open(tables_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'table_name', 'schema_name', 'catalog_name', 'full_name',
            'table_type', 'data_source_format', 'table_format',
            'is_delta', 'is_iceberg', 'is_uniform',
            'storage_location', 'owner', 'comment',
            'size_bytes', 'num_files',
            'metastore_id', 'metastore_name', 'workspace_id', 'workspace_name',
            'region', 'created_at', 'created_by', 'updated_at', 'updated_by',
            'collected_at', 'collection_timestamp'
        ])
        
        for tbl in tables:
            full_name = f"{tbl.get('catalog_name', '')}.{tbl.get('schema_name', '')}.{tbl.get('table_name', '')}"
            writer.writerow([
                tbl.get('table_name', ''),
                tbl.get('schema_name', ''),
                tbl.get('catalog_name', ''),
                full_name,
                tbl.get('table_type', ''),
                tbl.get('data_source_format', ''),
                tbl.get('table_format', ''),
                tbl.get('is_delta', False),
                tbl.get('is_iceberg', False),
                tbl.get('is_uniform', False),
                tbl.get('storage_location', ''),
                tbl.get('owner', ''),
                (tbl.get('comment', '') or '').replace('\n', ' '),
                tbl.get('size_bytes', ''),
                tbl.get('num_files', ''),
                tbl.get('metastore_id', ''),
                tbl.get('metastore_name', ''),
                tbl.get('workspace_id', ''),
                tbl.get('workspace_name', ''),
                tbl.get('region', ''),
                tbl.get('created_at', ''),
                tbl.get('created_by', ''),
                tbl.get('updated_at', ''),
                tbl.get('updated_by', ''),
                tbl.get('collected_at', ''),
                collection_ts
            ])
    
    logger.info(f"    Saved {len(tables)} tables to {os.path.basename(tables_file)}")
    return tables_file


def export_catalog_schemas_csv(schemas: List[Dict[str, Any]], catalog_name: str,
                                output_dir: str, timestamp: str, collection_ts: str) -> str:
    """Export schemas for a single catalog to CSV (incremental save)."""
    os.makedirs(output_dir, exist_ok=True)
    safe_catalog_name = catalog_name.replace('/', '_').replace('\\', '_').replace(' ', '_')
    schemas_file = os.path.join(output_dir, f'collect_schemas_{safe_catalog_name}_{timestamp}.csv')
    
    with open(schemas_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'schema_name', 'catalog_name', 'owner', 'comment',
            'workspace_id', 'workspace_name',
            'created_at', 'created_by', 'updated_at', 'updated_by',
            'collection_timestamp'
        ])
        
        for schema in schemas:
            writer.writerow([
                schema.get('schema_name', ''),
                schema.get('catalog_name', ''),
                schema.get('owner', ''),
                (schema.get('comment', '') or '').replace('\n', ' '),
                schema.get('workspace_id', ''),
                schema.get('workspace_name', ''),
                schema.get('created_at', ''),
                schema.get('created_by', ''),
                schema.get('updated_at', ''),
                schema.get('updated_by', ''),
                collection_ts
            ])
    
    logger.info(f"    Saved {len(schemas)} schemas to {os.path.basename(schemas_file)}")
    return schemas_file


def export_catalog_volumes_csv(volumes: List[Dict[str, Any]], catalog_name: str,
                                output_dir: str, timestamp: str, collection_ts: str) -> str:
    """Export volumes for a single catalog to CSV (incremental save)."""
    os.makedirs(output_dir, exist_ok=True)
    safe_catalog_name = catalog_name.replace('/', '_').replace('\\', '_').replace(' ', '_')
    volumes_file = os.path.join(output_dir, f'collect_volumes_{safe_catalog_name}_{timestamp}.csv')
    
    with open(volumes_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'volume_name', 'schema_name', 'catalog_name', 'full_name',
            'volume_type', 'storage_location', 'owner', 'comment',
            'size_bytes',
            'metastore_id', 'metastore_name', 'workspace_id', 'workspace_name',
            'region', 'created_at', 'created_by', 'updated_at', 'updated_by',
            'collected_at', 'collection_timestamp'
        ])
        
        for vol in volumes:
            full_name = f"{vol.get('catalog_name', '')}.{vol.get('schema_name', '')}.{vol.get('volume_name', '')}"
            writer.writerow([
                vol.get('volume_name', ''),
                vol.get('schema_name', ''),
                vol.get('catalog_name', ''),
                full_name,
                vol.get('volume_type', ''),
                vol.get('storage_location', ''),
                vol.get('owner', ''),
                (vol.get('comment', '') or '').replace('\n', ' '),
                vol.get('size_bytes', ''),
                vol.get('metastore_id', ''),
                vol.get('metastore_name', ''),
                vol.get('workspace_id', ''),
                vol.get('workspace_name', ''),
                vol.get('region', ''),
                vol.get('created_at', ''),
                vol.get('created_by', ''),
                vol.get('updated_at', ''),
                vol.get('updated_by', ''),
                vol.get('collected_at', ''),
                collection_ts
            ])
    
    logger.info(f"    Saved {len(volumes)} volumes to {os.path.basename(volumes_file)}")
    return volumes_file


def export_scan_to_csv(scan_data: Dict[str, Any], output_dir: str, timestamp: str = None,
                       skip_workspaces: bool = False) -> Dict[str, str]:
    """Export scan results to CSV files (prefixed with 'account_').
    
    Args:
        scan_data: Scan result dictionary
        output_dir: Output directory
        timestamp: Timestamp for filenames
        skip_workspaces: If True, skip workspaces CSV (used during collect where workspaces are merged)
    """
    
    if not timestamp:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    os.makedirs(output_dir, exist_ok=True)
    exported_files = {}
    
    # 1. Summary CSV
    summary_file = os.path.join(output_dir, f'account_summary_{timestamp}.csv')
    with open(summary_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|')
        writer.writerow(['metric', 'value', 'scan_timestamp'])
        
        summary = scan_data.get('summary', {})
        scan_ts = scan_data.get('scan_timestamp', '')
        
        writer.writerow(['account_id', scan_data.get('subscription_id', ''), scan_ts])
        writer.writerow(['total_workspaces', summary.get('total_workspaces', 0), scan_ts])
        writer.writerow(['uc_workspaces', summary.get('uc_workspaces', 0), scan_ts])
        writer.writerow(['hms_only_workspaces', summary.get('hms_only_workspaces', 0), scan_ts])
        writer.writerow(['total_metastores', summary.get('total_metastores', 0), scan_ts])
        writer.writerow(['errors_count', summary.get('errors_count', 0), scan_ts])
    
    exported_files['summary'] = summary_file
    logger.info(f"  Exported summary to {summary_file}")
    
    # 2. Workspaces CSV (skip during collect - workspaces are merged into collect_workspaces)
    if not skip_workspaces:
        workspaces_file = os.path.join(output_dir, f'account_workspaces_{timestamp}.csv')
        with open(workspaces_file, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
            writer.writerow([
                'workspace_id', 'workspace_name', 'workspace_url', 'workspace_type', 'workspace_status',
                'has_unity_catalog', 'metastore_id', 'metastore_name', 'sku',
                'cloud', 'cloud_region',
                'subscription_id', 'resource_group',
                'aws_account_id', 'credentials_id', 'storage_configuration_id',
                'gcp_project_id',
                'cmk_managed_services', 'cmk_managed_disk', 'cmk_dbfs', 'cmk_key_vault_uri', 'cmk_key_name',
                'admin_user_count', 'admin_group_count', 'admin_users', 'admin_groups',
                'ip_access_list_enabled', 'ip_access_list_count',
                'private_access_enabled', 'serverless_private_endpoint_count',
                'serverless_compute_enabled', 'model_serving_enabled', 'tokens_enabled',
                'max_token_lifetime_days', 'results_downloading_enabled', 'web_terminal_enabled',
                'user_isolation_enabled', 'feature_count',
                'collected_at', 'scan_timestamp'
            ])
            
            for ws in scan_data.get('workspaces', []):
                writer.writerow([
                    ws.get('workspace_id', ''),
                    ws.get('workspace_name', ''),
                    ws.get('workspace_url', ''),
                    ws.get('workspace_type', ''),
                    ws.get('workspace_status', ''),
                    ws.get('has_unity_catalog', False),
                    ws.get('metastore_id', ''),
                    ws.get('metastore_name', ''),
                    ws.get('sku', ''),
                    ws.get('cloud', 'unknown'),
                    ws.get('cloud_region', '') or ws.get('region', ''),
                    ws.get('subscription_id', ''),
                    ws.get('resource_group', ''),
                    ws.get('aws_account_id', ''),
                    ws.get('credentials_id', ''),
                    ws.get('storage_configuration_id', ''),
                    ws.get('gcp_project_id', ''),
                    ws.get('cmk_managed_services_enabled', False),
                    ws.get('cmk_managed_disk_enabled', False),
                    ws.get('cmk_dbfs_enabled', False),
                    ws.get('cmk_key_vault_uri', ''),
                    ws.get('cmk_key_name', ''),
                    ws.get('admin_user_count', 0),
                    ws.get('admin_group_count', 0),
                    ','.join(ws.get('admin_users', []) or []),
                    ','.join(ws.get('admin_groups', []) or []),
                    ws.get('ip_access_list_enabled', False),
                    ws.get('ip_access_list_count', 0),
                    ws.get('private_access_enabled', False),
                    ws.get('serverless_private_endpoint_count', 0),
                    ws.get('serverless_compute_enabled', False),
                    ws.get('model_serving_enabled', False),
                    ws.get('tokens_enabled', False),
                    ws.get('max_token_lifetime_days', ''),
                    ws.get('results_downloading_enabled', False),
                    ws.get('web_terminal_enabled', False),
                    ws.get('user_isolation_enabled', False),
                    len(ws.get('workspace_features', {})),
                    ws.get('collected_at', ''),
                    scan_data.get('scan_timestamp', '')
                ])
        
        exported_files['workspaces'] = workspaces_file
        logger.info(f"  Exported {len(scan_data.get('workspaces', []))} workspaces")
    
    # 3. Metastores CSV
    metastores_file = os.path.join(output_dir, f'account_metastores_{timestamp}.csv')
    with open(metastores_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'metastore_id', 'metastore_name', 'region',
            'workspace_count', 'workspaces', 'suggested_admin_workspace',
            'scan_timestamp'
        ])
        
        for ms in scan_data.get('metastores', []):
            writer.writerow([
                ms.get('metastore_id', ''),
                ms.get('metastore_name', ''),
                ms.get('region', ''),
                ms.get('workspace_count', 0),
                ','.join(ms.get('workspaces', [])),
                ms.get('suggested_admin_workspace', ''),
                scan_data.get('scan_timestamp', '')
            ])
    
    exported_files['metastores'] = metastores_file
    logger.info(f"  Exported {len(scan_data.get('metastores', []))} metastores")
    
    # 4. Errors CSV
    errors_file = os.path.join(output_dir, f'account_errors_{timestamp}.csv')
    with open(errors_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['workspace', 'workspace_id', 'error_type', 'message', 'scan_timestamp'])
        
        for err in scan_data.get('errors', []):
            writer.writerow([
                err.get('workspace', ''),
                err.get('workspace_id', ''),
                err.get('error', ''),
                err.get('message', '').replace('\n', ' '),
                scan_data.get('scan_timestamp', '')
            ])
    
    exported_files['errors'] = errors_file
    logger.info(f"  Exported {len(scan_data.get('errors', []))} errors")
    
    return exported_files


def export_collection_to_csv(collection_data: Dict[str, Any], output_dir: str, timestamp: str = None) -> Dict[str, str]:
    """Export collection results to CSV files (prefixed with 'collect_')"""
    
    if not timestamp:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    os.makedirs(output_dir, exist_ok=True)
    exported_files = {}
    collection_ts = collection_data.get('collection_timestamp', '')
    
    # Unified workspaces file (merges account-level + collect-level details)
    workspaces_file = os.path.join(output_dir, f'collect_workspaces_{timestamp}.csv')
    with open(workspaces_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'workspace_id', 'workspace_name', 'workspace_url', 'workspace_type', 'workspace_status',
            'has_unity_catalog', 'metastore_id', 'metastore_name', 'sku',
            # Multi-cloud fields
            'cloud', 'cloud_region',
            # Azure-specific
            'subscription_id', 'resource_group',
            # AWS-specific
            'aws_account_id', 'credentials_id', 'storage_configuration_id',
            # GCP-specific
            'gcp_project_id',
            # Security
            'cmk_managed_services', 'cmk_managed_disk', 'cmk_dbfs', 'cmk_key_vault_uri', 'cmk_key_name',
            'admin_user_count', 'admin_group_count', 'admin_users', 'admin_groups',
            'ip_access_list_enabled', 'ip_access_list_count',
            'private_access_enabled', 'serverless_private_endpoint_count',
            # Workspace Features
            # Workspace Features (key fields - see workspace_features CSV for full list)
            'serverless_compute_enabled', 'model_serving_enabled', 'tokens_enabled',
            'max_token_lifetime_days', 'results_downloading_enabled', 'web_terminal_enabled',
            'user_isolation_enabled', 'feature_count',
            'collected_at', 'collection_timestamp'
        ])
        
        for ws in collection_data.get('workspaces', []):
            writer.writerow([
                ws.get('workspace_id', ''),
                ws.get('workspace_name', ''),
                ws.get('workspace_url', ''),
                ws.get('workspace_type', ''),
                ws.get('workspace_status', ''),
                ws.get('has_unity_catalog', False),
                ws.get('metastore_id', ''),
                ws.get('metastore_name', ''),
                ws.get('sku', ''),
                # Multi-cloud
                ws.get('cloud', 'unknown'),
                ws.get('cloud_region', '') or ws.get('region', ''),
                # Azure
                ws.get('subscription_id', ''),
                ws.get('resource_group', ''),
                # AWS
                ws.get('aws_account_id', ''),
                ws.get('credentials_id', ''),
                ws.get('storage_configuration_id', ''),
                # GCP
                ws.get('gcp_project_id', ''),
                # Security
                ws.get('cmk_managed_services_enabled', False),
                ws.get('cmk_managed_disk_enabled', False),
                ws.get('cmk_dbfs_enabled', False),
                ws.get('cmk_key_vault_uri', ''),
                ws.get('cmk_key_name', ''),
                ws.get('admin_user_count', 0),
                ws.get('admin_group_count', 0),
                ','.join(ws.get('admin_users', []) or []),
                ','.join(ws.get('admin_groups', []) or []),
                ws.get('ip_access_list_enabled', False),
                ws.get('ip_access_list_count', 0),
                ws.get('private_access_enabled', False),
                ws.get('serverless_private_endpoint_count', 0),
                # Workspace Features
                ws.get('serverless_compute_enabled', False),
                ws.get('model_serving_enabled', False),
                ws.get('tokens_enabled', False),
                ws.get('max_token_lifetime_days', ''),
                ws.get('results_downloading_enabled', False),
                ws.get('web_terminal_enabled', False),
                ws.get('user_isolation_enabled', False),
                len(ws.get('workspace_features', {})),  # feature_count
                ws.get('collected_at', ''),
                collection_ts
            ])
    
    exported_files['workspaces'] = workspaces_file
    logger.info(f"  Exported {len(collection_data.get('workspaces', []))} workspaces")
    
    # 1. Catalogs CSV
    catalogs_file = os.path.join(output_dir, f'collect_catalogs_{timestamp}.csv')
    with open(catalogs_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'catalog_name', 'catalog_type', 'owner', 'comment',
            'metastore_id', 'metastore_name', 'workspace_id', 'workspace_name',
            'is_legacy_hms', 'region',
            'created_at', 'created_by', 'updated_at', 'updated_by',
            'collected_at', 'collection_timestamp'
        ])
        
        for cat in collection_data.get('catalogs', []):
            writer.writerow([
                cat.get('catalog_name', ''),
                cat.get('catalog_type', ''),
                cat.get('owner', ''),
                (cat.get('comment', '') or '').replace('\n', ' '),
                cat.get('metastore_id', ''),
                cat.get('metastore_name', ''),
                cat.get('workspace_id', ''),
                cat.get('workspace_name', ''),
                cat.get('is_legacy_hms', False),
                cat.get('region', ''),
                cat.get('created_at', ''),
                cat.get('created_by', ''),
                cat.get('updated_at', ''),
                cat.get('updated_by', ''),
                cat.get('collected_at', ''),
                collection_ts
            ])
    
    exported_files['catalogs'] = catalogs_file
    logger.info(f"  Exported {len(collection_data.get('catalogs', []))} catalogs")
    
    # Schemas, Tables, Volumes saved per-catalog
    logger.info("  Schemas/Tables/Volumes: saved per-catalog")
    
    # 2. External Locations CSV
    ext_loc_file = os.path.join(output_dir, f'collect_external_locations_{timestamp}.csv')
    with open(ext_loc_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'location_name', 'url', 'credential_name', 'storage_account', 'storage_type',
            'owner', 'comment', 'read_only',
            'metastore_id', 'metastore_name', 'workspace_id', 'workspace_name',
            'region', 'created_at', 'created_by', 'updated_at', 'updated_by',
            'collected_at', 'collection_timestamp'
        ])
        
        for loc in collection_data.get('external_locations', []):
            writer.writerow([
                loc.get('location_name', ''),
                loc.get('url', ''),
                loc.get('credential_name', ''),
                loc.get('storage_account', ''),
                loc.get('storage_type', ''),
                loc.get('owner', ''),
                (loc.get('comment', '') or '').replace('\n', ' '),
                loc.get('read_only', False),
                loc.get('metastore_id', ''),
                loc.get('metastore_name', ''),
                loc.get('workspace_id', ''),
                loc.get('workspace_name', ''),
                loc.get('region', ''),
                loc.get('created_at', ''),
                loc.get('created_by', ''),
                loc.get('updated_at', ''),
                loc.get('updated_by', ''),
                loc.get('collected_at', ''),
                collection_ts
            ])
    
    exported_files['external_locations'] = ext_loc_file
    logger.info(f"  Exported {len(collection_data.get('external_locations', []))} external locations")
    
    # 6. Catalog Bindings CSV
    bindings_file = os.path.join(output_dir, f'collect_catalog_bindings_{timestamp}.csv')
    with open(bindings_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'catalog_name', 'workspace_id', 'binding_type',
            'metastore_id', 'metastore_name',
            'collection_timestamp'
        ])
        
        for binding in collection_data.get('catalog_bindings', []):
            writer.writerow([
                binding.get('catalog_name', ''),
                binding.get('workspace_id', ''),
                binding.get('binding_type', ''),
                binding.get('metastore_id', ''),
                binding.get('metastore_name', ''),
                collection_ts
            ])
    
    exported_files['catalog_bindings'] = bindings_file
    logger.info(f"  Exported {len(collection_data.get('catalog_bindings', []))} catalog bindings")
    
    # 7. External Location Bindings CSV
    ext_loc_bindings_file = os.path.join(output_dir, f'collect_external_location_bindings_{timestamp}.csv')
    with open(ext_loc_bindings_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'location_name', 'workspace_id', 'workspace_name', 'binding_type',
            'metastore_id', 'metastore_name',
            'collection_timestamp'
        ])
        
        for binding in collection_data.get('external_location_bindings', []):
            writer.writerow([
                binding.get('location_name', ''),
                binding.get('workspace_id', ''),
                binding.get('workspace_name', ''),
                binding.get('binding_type', ''),
                binding.get('metastore_id', ''),
                binding.get('metastore_name', ''),
                collection_ts
            ])
    
    exported_files['external_location_bindings'] = ext_loc_bindings_file
    logger.info(f"  Exported {len(collection_data.get('external_location_bindings', []))} external location bindings")
    
    # 8. Connections (Federated Sources) CSV
    connections_file = os.path.join(output_dir, f'collect_connections_{timestamp}.csv')
    with open(connections_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'connection_name', 'connection_type', 'host', 'port',
            'owner', 'comment', 'read_only',
            'metastore_id', 'metastore_name', 'workspace_id', 'workspace_name',
            'created_at', 'created_by', 'updated_at', 'updated_by',
            'collection_timestamp'
        ])
        
        for conn in collection_data.get('connections', []):
            writer.writerow([
                conn.get('connection_name', ''),
                conn.get('connection_type', ''),
                conn.get('host', ''),
                conn.get('port', ''),
                conn.get('owner', ''),
                (conn.get('comment', '') or '').replace('\n', ' '),
                conn.get('read_only', False),
                conn.get('metastore_id', ''),
                conn.get('metastore_name', ''),
                conn.get('workspace_id', ''),
                conn.get('workspace_name', ''),
                conn.get('created_at', ''),
                conn.get('created_by', ''),
                conn.get('updated_at', ''),
                conn.get('updated_by', ''),
                collection_ts
            ])
    
    exported_files['connections'] = connections_file
    logger.info(f"  Exported {len(collection_data.get('connections', []))} connections")
    
    # 9. Delta Shares CSV
    shares_file = os.path.join(output_dir, f'collect_delta_shares_{timestamp}.csv')
    with open(shares_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'share_name', 'owner', 'comment',
            'object_count', 'table_count', 'schema_count',
            'shared_tables', 'recipient_count', 'recipients', 'total_size_bytes',
            'metastore_id', 'metastore_name', 'workspace_id', 'workspace_name',
            'region', 'created_at', 'created_by', 'updated_at', 'updated_by',
            'collected_at', 'collection_timestamp'
        ])
        
        for share in collection_data.get('shares', []):
            writer.writerow([
                share.get('share_name', ''),
                share.get('owner', ''),
                (share.get('comment', '') or '').replace('\n', ' '),
                share.get('object_count', 0),
                share.get('table_count', 0),
                share.get('schema_count', 0),
                ','.join(share.get('shared_tables', [])),
                share.get('recipient_count', 0),
                ','.join(share.get('recipients', [])),
                share.get('total_size_bytes', ''),
                share.get('metastore_id', ''),
                share.get('metastore_name', ''),
                share.get('workspace_id', ''),
                share.get('workspace_name', ''),
                share.get('region', ''),
                share.get('created_at', ''),
                share.get('created_by', ''),
                share.get('updated_at', ''),
                share.get('updated_by', ''),
                share.get('collected_at', ''),
                collection_ts
            ])
    
    exported_files['delta_shares'] = shares_file
    logger.info(f"  Exported {len(collection_data.get('shares', []))} delta shares")
    
    # 8. Repos CSV
    repos_file = os.path.join(output_dir, f'collect_repos_{timestamp}.csv')
    with open(repos_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'repo_id', 'repo_path', 'url', 'provider', 'branch', 'head_commit_id',
            'workspace_id', 'workspace_name',
            'collection_timestamp'
        ])
        
        for repo in collection_data.get('repos', []):
            writer.writerow([
                repo.get('repo_id', ''),
                repo.get('repo_path', ''),
                repo.get('url', ''),
                repo.get('provider', ''),
                repo.get('branch', ''),
                repo.get('head_commit_id', ''),
                repo.get('workspace_id', ''),
                repo.get('workspace_name', ''),
                collection_ts
            ])
    
    exported_files['repos'] = repos_file
    logger.info(f"  Exported {len(collection_data.get('repos', []))} repos")
    
    # 9. Workspace Features CSV (normalized - one row per feature)
    features_file = os.path.join(output_dir, f'collect_workspace_features_{timestamp}.csv')
    with open(features_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            'workspace_id', 'workspace_name', 'feature_name', 'feature_value',
            'metastore_id', 'collection_timestamp'
        ])
        
        feature_count = 0
        for ws in collection_data.get('workspaces', []):
            workspace_id = ws.get('workspace_id', '')
            workspace_name = ws.get('workspace_name', '')
            metastore_id = ws.get('metastore_id', '')
            features = ws.get('workspace_features', {})
            
            for feature_name, feature_value in features.items():
                writer.writerow([
                    workspace_id,
                    workspace_name,
                    feature_name,
                    feature_value,
                    metastore_id,
                    collection_ts
                ])
                feature_count += 1
    
    exported_files['workspace_features'] = features_file
    logger.info(f"  Exported {feature_count} workspace features")
    
    return exported_files

