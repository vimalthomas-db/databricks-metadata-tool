"""Write scan and collection results to Delta tables in a Databricks workspace"""

import logging
import os
from typing import Dict, Any, List
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

logger = logging.getLogger('databricks_metadata_tool.delta_writer')


# Table schema definitions - single source of truth
SCAN_TABLES = {
    'scan_summary': [
        ('metric', 'STRING'),
        ('value', 'STRING'),
        ('scan_timestamp', 'TIMESTAMP'),
        ('load_timestamp', 'TIMESTAMP'),
    ],
    'workspaces': [
        ('workspace_id', 'STRING'),
        ('workspace_name', 'STRING'),
        ('workspace_url', 'STRING'),
        ('workspace_type', 'STRING'),
        ('has_unity_catalog', 'BOOLEAN'),
        ('metastore_id', 'STRING'),
        ('metastore_name', 'STRING'),
        ('region', 'STRING'),
        ('sku', 'STRING'),
        ('subscription_id', 'STRING'),
        ('resource_group', 'STRING'),
        ('location', 'STRING'),
        ('cmk_managed_services', 'BOOLEAN'),
        ('cmk_managed_disk', 'BOOLEAN'),
        ('cmk_dbfs', 'BOOLEAN'),
        ('cmk_key_vault_uri', 'STRING'),
        ('cmk_key_name', 'STRING'),
        ('admin_user_count', 'INT'),
        ('admin_group_count', 'INT'),
        ('ip_access_list_enabled', 'BOOLEAN'),
        ('ip_access_list_count', 'INT'),
        ('private_access_enabled', 'BOOLEAN'),
        ('serverless_private_endpoint_count', 'INT'),
        ('collected_at', 'TIMESTAMP'),
        ('scan_timestamp', 'TIMESTAMP'),
        ('load_timestamp', 'TIMESTAMP'),
    ],
    'metastores': [
        ('metastore_id', 'STRING'),
        ('metastore_name', 'STRING'),
        ('region', 'STRING'),
        ('workspace_count', 'INT'),
        ('workspaces', 'STRING'),
        ('suggested_admin_workspace', 'STRING'),
        ('scan_timestamp', 'TIMESTAMP'),
        ('load_timestamp', 'TIMESTAMP'),
    ],
    'scan_errors': [
        ('workspace', 'STRING'),
        ('workspace_id', 'STRING'),
        ('error_type', 'STRING'),
        ('message', 'STRING'),
        ('scan_timestamp', 'TIMESTAMP'),
        ('load_timestamp', 'TIMESTAMP'),
    ],
}

COLLECTION_TABLES = {
    'catalogs': [
        ('catalog_name', 'STRING'),
        ('catalog_type', 'STRING'),
        ('owner', 'STRING'),
        ('comment', 'STRING'),
        ('metastore_id', 'STRING'),
        ('metastore_name', 'STRING'),
        ('workspace_id', 'STRING'),
        ('workspace_name', 'STRING'),
        ('is_legacy_hms', 'BOOLEAN'),
        ('region', 'STRING'),
        ('created_at', 'BIGINT'),
        ('created_by', 'STRING'),
        ('collected_at', 'TIMESTAMP'),
        ('collection_timestamp', 'TIMESTAMP'),
        ('load_timestamp', 'TIMESTAMP'),
    ],
    'schemas': [
        ('schema_name', 'STRING'),
        ('catalog_name', 'STRING'),
        ('full_name', 'STRING'),
        ('owner', 'STRING'),
        ('comment', 'STRING'),
        ('metastore_id', 'STRING'),
        ('metastore_name', 'STRING'),
        ('workspace_id', 'STRING'),
        ('workspace_name', 'STRING'),
        ('created_at', 'BIGINT'),
        ('created_by', 'STRING'),
        ('collection_timestamp', 'TIMESTAMP'),
        ('load_timestamp', 'TIMESTAMP'),
    ],
    'tables': [
        ('table_name', 'STRING'),
        ('schema_name', 'STRING'),
        ('catalog_name', 'STRING'),
        ('full_name', 'STRING'),
        ('table_type', 'STRING'),
        ('data_source_format', 'STRING'),
        ('table_format', 'STRING'),
        ('is_delta', 'BOOLEAN'),
        ('is_iceberg', 'BOOLEAN'),
        ('is_uniform', 'BOOLEAN'),
        ('storage_location', 'STRING'),
        ('owner', 'STRING'),
        ('comment', 'STRING'),
        ('size_bytes', 'BIGINT'),
        ('num_files', 'INT'),
        ('metastore_id', 'STRING'),
        ('metastore_name', 'STRING'),
        ('workspace_id', 'STRING'),
        ('workspace_name', 'STRING'),
        ('region', 'STRING'),
        ('created_at', 'BIGINT'),
        ('created_by', 'STRING'),
        ('collected_at', 'TIMESTAMP'),
        ('collection_timestamp', 'TIMESTAMP'),
        ('load_timestamp', 'TIMESTAMP'),
    ],
    'volumes': [
        ('volume_name', 'STRING'),
        ('schema_name', 'STRING'),
        ('catalog_name', 'STRING'),
        ('full_name', 'STRING'),
        ('volume_type', 'STRING'),
        ('storage_location', 'STRING'),
        ('owner', 'STRING'),
        ('comment', 'STRING'),
        ('size_bytes', 'BIGINT'),
        ('metastore_id', 'STRING'),
        ('metastore_name', 'STRING'),
        ('workspace_id', 'STRING'),
        ('workspace_name', 'STRING'),
        ('region', 'STRING'),
        ('collected_at', 'TIMESTAMP'),
        ('collection_timestamp', 'TIMESTAMP'),
        ('load_timestamp', 'TIMESTAMP'),
    ],
    'external_locations': [
        ('location_name', 'STRING'),
        ('url', 'STRING'),
        ('credential_name', 'STRING'),
        ('storage_account', 'STRING'),
        ('storage_type', 'STRING'),
        ('owner', 'STRING'),
        ('comment', 'STRING'),
        ('read_only', 'BOOLEAN'),
        ('metastore_id', 'STRING'),
        ('metastore_name', 'STRING'),
        ('workspace_id', 'STRING'),
        ('workspace_name', 'STRING'),
        ('region', 'STRING'),
        ('collected_at', 'TIMESTAMP'),
        ('collection_timestamp', 'TIMESTAMP'),
        ('load_timestamp', 'TIMESTAMP'),
    ],
    'catalog_bindings': [
        ('catalog_name', 'STRING'),
        ('workspace_id', 'STRING'),
        ('binding_type', 'STRING'),
        ('metastore_id', 'STRING'),
        ('metastore_name', 'STRING'),
        ('collection_timestamp', 'TIMESTAMP'),
        ('load_timestamp', 'TIMESTAMP'),
    ],
    'delta_shares': [
        ('share_name', 'STRING'),
        ('owner', 'STRING'),
        ('comment', 'STRING'),
        ('object_count', 'INT'),
        ('table_count', 'INT'),
        ('schema_count', 'INT'),
        ('shared_tables', 'STRING'),
        ('recipient_count', 'INT'),
        ('recipients', 'STRING'),
        ('total_size_bytes', 'BIGINT'),
        ('metastore_id', 'STRING'),
        ('metastore_name', 'STRING'),
        ('workspace_id', 'STRING'),
        ('workspace_name', 'STRING'),
        ('region', 'STRING'),
        ('created_at', 'BIGINT'),
        ('created_by', 'STRING'),
        ('collected_at', 'TIMESTAMP'),
        ('collection_timestamp', 'TIMESTAMP'),
        ('load_timestamp', 'TIMESTAMP'),
    ],
    'repos': [
        ('repo_id', 'BIGINT'),
        ('repo_path', 'STRING'),
        ('url', 'STRING'),
        ('provider', 'STRING'),
        ('branch', 'STRING'),
        ('head_commit_id', 'STRING'),
        ('workspace_id', 'STRING'),
        ('workspace_name', 'STRING'),
        ('collection_timestamp', 'TIMESTAMP'),
        ('load_timestamp', 'TIMESTAMP'),
    ],
}


class DeltaWriter:
    
    def __init__(self, workspace_url: str, catalog: str, schema: str, warehouse_id: str):
        self.workspace_url = workspace_url
        self.catalog = catalog
        self.schema = schema
        self.warehouse_id = warehouse_id
        
        self.client = WorkspaceClient(
            host=workspace_url,
            azure_client_id=os.getenv('AZURE_CLIENT_ID'),
            azure_tenant_id=os.getenv('AZURE_TENANT_ID'),
            azure_client_secret=os.getenv('AZURE_CLIENT_SECRET')
        )
        
        logger.info(f"Delta Writer initialized for {catalog}.{schema}")
    
    def _escape_sql_value(self, val: Any) -> str:
        """Safely escape a value for SQL insertion"""
        if val is None:
            return 'NULL'
        if isinstance(val, bool):
            return 'true' if val else 'false'
        if isinstance(val, (int, float)):
            return str(val)
        
        s = str(val)
        s = s.replace('\\', '\\\\')
        s = s.replace("'", "''")
        s = s.replace('\n', ' ')
        s = s.replace('\r', '')
        return f"'{s}'"
    
    def _execute_sql(self, sql: str, timeout: str = '50s') -> Any:
        """Execute SQL statement via warehouse with validation"""
        try:
            logger.debug(f"Executing SQL: {sql[:200]}...")
            response = self.client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=sql,
                wait_timeout=timeout
            )
            
            if response.status and response.status.state == StatementState.FAILED:
                error_msg = response.status.error.message if response.status.error else 'Unknown error'
                logger.error(f"SQL failed: {error_msg}")
                raise RuntimeError(f"SQL execution failed: {error_msg}")
            
            return response
        except Exception as e:
            logger.error(f"SQL execution error: {str(e)}")
            raise
    
    def ensure_schema_exists(self):
        """Create schema if it doesn't exist"""
        logger.info(f"Ensuring schema {self.catalog}.{self.schema} exists...")
        try:
            self._execute_sql(f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{self.schema}`")
            logger.info(f"  Schema {self.catalog}.{self.schema} ready")
        except Exception as e:
            logger.error(f"Failed to create schema: {str(e)}")
            raise
    
    def _create_table(self, table_name: str, columns: List[tuple]):
        """Create Delta table if it doesn't exist"""
        full_name = f"`{self.catalog}`.`{self.schema}`.`{table_name}`"
        col_defs = ', '.join([f"`{name}` {dtype}" for name, dtype in columns])
        
        sql = f"""
        CREATE TABLE IF NOT EXISTS {full_name} (
            {col_defs}
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        
        try:
            self._execute_sql(sql)
            logger.debug(f"  Table {full_name} ready")
        except Exception as e:
            logger.error(f"Failed to create table {full_name}: {str(e)}")
            raise
    
    def _insert_rows(self, table_name: str, columns: List[str], rows: List[List[Any]], batch_size: int = 50):
        """Insert rows into Delta table in batches"""
        if not rows:
            logger.debug(f"  No data to insert into {table_name}")
            return
        
        full_name = f"`{self.catalog}`.`{self.schema}`.`{table_name}`"
        col_list = ', '.join([f"`{c}`" for c in columns])
        
        total_inserted = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            
            values_list = []
            for row in batch:
                values = [self._escape_sql_value(val) for val in row]
                values_list.append(f"({', '.join(values)})")
            
            sql = f"INSERT INTO {full_name} ({col_list}) VALUES {', '.join(values_list)}"
            
            try:
                self._execute_sql(sql, timeout='50s')
                total_inserted += len(batch)
            except Exception as e:
                logger.error(f"Failed to insert batch into {full_name}: {str(e)}")
                raise
        
        logger.info(f"  Inserted {total_inserted} rows into {table_name}")
    
    def _get_column_names(self, table_schema: List[tuple]) -> List[str]:
        """Extract column names from table schema"""
        return [col[0] for col in table_schema]

    def write_scan_results(self, scan_data: Dict[str, Any]):
        """Write scan results to Delta tables"""
        logger.info("=" * 60)
        logger.info(f"Writing scan results to {self.catalog}.{self.schema}")
        logger.info("=" * 60)
        
        self.ensure_schema_exists()
        
        scan_ts = scan_data.get('scan_timestamp', datetime.now().isoformat())
        load_ts = datetime.now().isoformat()
        
        for table_name, schema in SCAN_TABLES.items():
            self._create_table(table_name, schema)
        
        # Summary
        summary = scan_data.get('summary', {})
        summary_rows = [
            ['account_id', scan_data.get('subscription_id', ''), scan_ts, load_ts],
            ['total_workspaces', str(summary.get('total_workspaces', 0)), scan_ts, load_ts],
            ['uc_workspaces', str(summary.get('uc_workspaces', 0)), scan_ts, load_ts],
            ['hms_only_workspaces', str(summary.get('hms_only_workspaces', 0)), scan_ts, load_ts],
            ['total_metastores', str(summary.get('total_metastores', 0)), scan_ts, load_ts],
            ['errors_count', str(summary.get('errors_count', 0)), scan_ts, load_ts],
        ]
        self._insert_rows('scan_summary', self._get_column_names(SCAN_TABLES['scan_summary']), summary_rows)
        
        # Workspaces
        ws_rows = []
        for ws in scan_data.get('workspaces', []):
            ws_rows.append([
                ws.get('workspace_id', ''),
                ws.get('workspace_name', ''),
                ws.get('workspace_url', ''),
                ws.get('workspace_type', ''),
                ws.get('has_unity_catalog', False),
                ws.get('metastore_id'),
                ws.get('metastore_name'),
                ws.get('region', ''),
                ws.get('sku', ''),
                ws.get('subscription_id', ''),
                ws.get('resource_group', ''),
                ws.get('location', ''),
                ws.get('cmk_managed_services_enabled', False),
                ws.get('cmk_managed_disk_enabled', False),
                ws.get('cmk_dbfs_enabled', False),
                ws.get('cmk_key_vault_uri'),
                ws.get('cmk_key_name'),
                ws.get('admin_user_count', 0),
                ws.get('admin_group_count', 0),
                ws.get('ip_access_list_enabled', False),
                ws.get('ip_access_list_count', 0),
                ws.get('private_access_enabled', False),
                ws.get('serverless_private_endpoint_count', 0),
                ws.get('collected_at', ''),
                scan_ts,
                load_ts
            ])
        self._insert_rows('workspaces', self._get_column_names(SCAN_TABLES['workspaces']), ws_rows)
        
        # Metastores
        ms_rows = []
        for ms in scan_data.get('metastores', []):
            ms_rows.append([
                ms.get('metastore_id', ''),
                ms.get('metastore_name', ''),
                ms.get('region', ''),
                ms.get('workspace_count', 0),
                ','.join(ms.get('workspaces', [])),
                ms.get('suggested_admin_workspace'),
                scan_ts,
                load_ts
            ])
        self._insert_rows('metastores', self._get_column_names(SCAN_TABLES['metastores']), ms_rows)
        
        # Errors
        err_rows = []
        for err in scan_data.get('errors', []):
            err_rows.append([
                err.get('workspace', ''),
                err.get('workspace_id', ''),
                err.get('error', ''),
                (err.get('message', '') or '')[:500],
                scan_ts,
                load_ts
            ])
        self._insert_rows('scan_errors', self._get_column_names(SCAN_TABLES['scan_errors']), err_rows)
        
        logger.info("Scan results written to Delta tables")

    def write_collection_results(self, collection_data: Dict[str, Any]):
        """Write collection results to Delta tables"""
        logger.info("=" * 60)
        logger.info(f"Writing collection results to {self.catalog}.{self.schema}")
        logger.info("=" * 60)
        
        self.ensure_schema_exists()
        
        collection_ts = collection_data.get('collection_timestamp', datetime.now().isoformat())
        load_ts = datetime.now().isoformat()
        
        for table_name, schema in COLLECTION_TABLES.items():
            self._create_table(table_name, schema)
        
        # Catalogs
        cat_rows = []
        for cat in collection_data.get('catalogs', []):
            cat_rows.append([
                cat.get('catalog_name', ''),
                cat.get('catalog_type', ''),
                cat.get('owner'),
                (cat.get('comment') or '')[:500],
                cat.get('metastore_id'),
                cat.get('metastore_name'),
                cat.get('workspace_id', ''),
                cat.get('workspace_name', ''),
                cat.get('is_legacy_hms', False),
                cat.get('region', ''),
                cat.get('created_at'),
                cat.get('created_by'),
                cat.get('collected_at', ''),
                collection_ts,
                load_ts
            ])
        self._insert_rows('catalogs', self._get_column_names(COLLECTION_TABLES['catalogs']), cat_rows)
        
        # Schemas
        schema_rows = []
        for s in collection_data.get('schemas', []):
            full_name = f"{s.get('catalog_name', '')}.{s.get('schema_name', '')}"
            schema_rows.append([
                s.get('schema_name', ''),
                s.get('catalog_name', ''),
                full_name,
                s.get('owner'),
                (s.get('comment') or '')[:500],
                s.get('metastore_id'),
                s.get('metastore_name'),
                s.get('workspace_id', ''),
                s.get('workspace_name', ''),
                s.get('created_at'),
                s.get('created_by'),
                collection_ts,
                load_ts
            ])
        self._insert_rows('schemas', self._get_column_names(COLLECTION_TABLES['schemas']), schema_rows)
        
        # Tables
        tbl_rows = []
        for t in collection_data.get('tables', []):
            full_name = f"{t.get('catalog_name', '')}.{t.get('schema_name', '')}.{t.get('table_name', '')}"
            tbl_rows.append([
                t.get('table_name', ''),
                t.get('schema_name', ''),
                t.get('catalog_name', ''),
                full_name,
                t.get('table_type', ''),
                t.get('data_source_format', ''),
                t.get('table_format', ''),
                t.get('is_delta', False),
                t.get('is_iceberg', False),
                t.get('is_uniform', False),
                t.get('storage_location'),
                t.get('owner'),
                (t.get('comment') or '')[:500],
                t.get('size_bytes'),
                t.get('num_files'),
                t.get('metastore_id'),
                t.get('metastore_name'),
                t.get('workspace_id', ''),
                t.get('workspace_name', ''),
                t.get('region', ''),
                t.get('created_at'),
                t.get('created_by'),
                t.get('collected_at', ''),
                collection_ts,
                load_ts
            ])
        self._insert_rows('tables', self._get_column_names(COLLECTION_TABLES['tables']), tbl_rows)
        
        # Volumes
        vol_rows = []
        for v in collection_data.get('volumes', []):
            full_name = f"{v.get('catalog_name', '')}.{v.get('schema_name', '')}.{v.get('volume_name', '')}"
            vol_rows.append([
                v.get('volume_name', ''),
                v.get('schema_name', ''),
                v.get('catalog_name', ''),
                full_name,
                v.get('volume_type', ''),
                v.get('storage_location'),
                v.get('owner'),
                (v.get('comment') or '')[:500],
                v.get('size_bytes'),
                v.get('metastore_id'),
                v.get('metastore_name'),
                v.get('workspace_id', ''),
                v.get('workspace_name', ''),
                v.get('region', ''),
                v.get('collected_at', ''),
                collection_ts,
                load_ts
            ])
        self._insert_rows('volumes', self._get_column_names(COLLECTION_TABLES['volumes']), vol_rows)
        
        # External Locations
        loc_rows = []
        for loc in collection_data.get('external_locations', []):
            loc_rows.append([
                loc.get('location_name', ''),
                loc.get('url', ''),
                loc.get('credential_name'),
                loc.get('storage_account'),
                loc.get('storage_type'),
                loc.get('owner'),
                (loc.get('comment') or '')[:500],
                loc.get('read_only', False),
                loc.get('metastore_id'),
                loc.get('metastore_name'),
                loc.get('workspace_id', ''),
                loc.get('workspace_name', ''),
                loc.get('region', ''),
                loc.get('collected_at', ''),
                collection_ts,
                load_ts
            ])
        self._insert_rows('external_locations', self._get_column_names(COLLECTION_TABLES['external_locations']), loc_rows)
        
        # Catalog Bindings
        binding_rows = []
        for b in collection_data.get('catalog_bindings', []):
            binding_rows.append([
                b.get('catalog_name', ''),
                b.get('workspace_id', ''),
                b.get('binding_type', ''),
                b.get('metastore_id'),
                b.get('metastore_name'),
                collection_ts,
                load_ts
            ])
        self._insert_rows('catalog_bindings', self._get_column_names(COLLECTION_TABLES['catalog_bindings']), binding_rows)
        
        # Delta Shares
        share_rows = []
        for sh in collection_data.get('shares', []):
            share_rows.append([
                sh.get('share_name', ''),
                sh.get('owner'),
                (sh.get('comment') or '')[:500],
                sh.get('object_count', 0),
                sh.get('table_count', 0),
                sh.get('schema_count', 0),
                ','.join(sh.get('shared_tables', [])),
                sh.get('recipient_count', 0),
                ','.join(sh.get('recipients', [])),
                sh.get('total_size_bytes'),
                sh.get('metastore_id'),
                sh.get('metastore_name'),
                sh.get('workspace_id', ''),
                sh.get('workspace_name', ''),
                sh.get('region', ''),
                sh.get('created_at'),
                sh.get('created_by'),
                sh.get('collected_at', ''),
                collection_ts,
                load_ts
            ])
        self._insert_rows('delta_shares', self._get_column_names(COLLECTION_TABLES['delta_shares']), share_rows)
        
        # Repos
        repo_rows = []
        for r in collection_data.get('repos', []):
            repo_rows.append([
                r.get('repo_id'),
                r.get('repo_path', ''),
                r.get('url', ''),
                r.get('provider', ''),
                r.get('branch'),
                r.get('head_commit_id'),
                r.get('workspace_id', ''),
                r.get('workspace_name', ''),
                collection_ts,
                load_ts
            ])
        self._insert_rows('repos', self._get_column_names(COLLECTION_TABLES['repos']), repo_rows)
        
        logger.info("Collection results written to Delta tables")

    def write_results(self, data: Dict[str, Any]):
        """Auto-detect and write scan or collection results"""
        if 'scan_timestamp' in data:
            self.write_scan_results(data)
        elif 'collection_timestamp' in data:
            self.write_collection_results(data)
        else:
            if data.get('metastores') or data.get('workspaces'):
                self.write_scan_results(data)
            if data.get('catalogs') or data.get('tables'):
                self.write_collection_results(data)
