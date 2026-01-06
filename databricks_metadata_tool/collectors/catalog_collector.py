import logging
from typing import List, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SchemaInfo, TableInfo, VolumeInfo

from databricks_metadata_tool.models import Catalog, Schema as SchemaModel, Table as TableModel, Volume as VolumeModel
from databricks_metadata_tool.utils import parse_table_properties

logger = logging.getLogger('databricks_metadata_tool.catalog_collector')


class CatalogCollector:
    
    def __init__(self, catalog: Catalog, client: WorkspaceClient, workspace_url: str, 
                 include_column_details: bool = True, exclude_schemas: List[str] = None,
                 size_workers: int = 10):
        self.catalog = catalog
        self.client = client
        self.workspace_url = workspace_url
        self.include_column_details = include_column_details
        self.exclude_schemas = exclude_schemas or []
        self.size_workers = size_workers
        
        logger.debug(f"Catalog collector initialized for {catalog.catalog_name}")
    
    def list_schemas(self) -> List[SchemaModel]:
        logger.debug(f"Listing schemas in catalog: {self.catalog.catalog_name}")
        
        schemas = []
        
        try:
            schema_list = self.client.schemas.list(catalog_name=self.catalog.catalog_name)
            
            for sch in schema_list:
                schema = SchemaModel(
                    schema_name=sch.name,
                    catalog_name=sch.catalog_name,
                    owner=sch.owner,
                    comment=sch.comment,
                    properties=dict(sch.properties) if sch.properties else {},
                    created_at=sch.created_at,
                    created_by=sch.created_by,
                    updated_at=sch.updated_at,
                    updated_by=sch.updated_by,
                    workspace_id=self.catalog.workspace_id,
                    workspace_name=self.catalog.workspace_name
                )
                schemas.append(schema)
        
        except Exception as e:
            logger.error(f"Error listing schemas in {self.catalog.catalog_name}: {str(e)}")
            raise
        
        return schemas
    
    def list_all_tables(self, warehouse_id: str = None, cluster_id: str = None, 
                        size_threshold: int = 200, dry_run_sizes: bool = False) -> List[TableModel]:
        """
        List all tables with smart tiered size collection.
        
        Tier 1: Try bulk system tables (fastest)
        Tier 2: If < threshold tables, use parallel SQL warehouse queries
        Tier 3: If >= threshold tables, use Spark job (requires cluster_id)
        
        Args:
            warehouse_id: SQL Warehouse ID for size queries
            cluster_id: Cluster ID for Spark job fallback (optional)
            size_threshold: Table count threshold for Spark job (default: 200)
            dry_run_sizes: If True, show tier selection without collecting sizes
        """
        
        logger.debug(f"Listing tables in catalog: {self.catalog.catalog_name}")
        
        all_tables = []
        
        try:
            schemas = self.list_schemas()
            
            # Filter excluded schemas
            if self.exclude_schemas:
                original_count = len(schemas)
                schemas = [s for s in schemas if s.schema_name not in self.exclude_schemas]
                if len(schemas) < original_count:
                    logger.debug(f"    Excluded {original_count - len(schemas)} schema(s): {', '.join(self.exclude_schemas)}")
            
            size_map = {}
            bulk_success = False
            if warehouse_id and self.catalog.catalog_name not in ['system', 'samples']:
                if dry_run_sizes:
                    logger.info(f"    [DRY RUN] Tier 1 (bulk) - skipped")
                else:
                    size_map = self._get_table_sizes_bulk(warehouse_id)
                    bulk_success = len(size_map) > 0
            
            # Collect all tables
            for schema in schemas:
                try:
                    tables = self._list_tables_in_schema(schema.schema_name, warehouse_id, size_map if bulk_success else {})
                    all_tables.extend(tables)
                except Exception as e:
                    logger.warning(f"Error listing tables in schema {schema.schema_name}: {str(e)}")
                    continue
            
            # If bulk succeeded, we're done - sizes already populated
            if bulk_success:
                logger.info(f"    Tier 1 (bulk): Got sizes for {len(size_map)} tables")
                return all_tables
            
            # Count tables needing sizes
            tables_needing_size = [t for t in all_tables if t.is_delta and t.table_type != 'VIEW' and t.size_bytes is None]
            
            if not tables_needing_size:
                return all_tables
            
            # Skip size collection if no warehouse_id provided
            if not warehouse_id:
                logger.debug(f"    Size collection skipped (no warehouse_id)")
                return all_tables
            
            if len(tables_needing_size) < size_threshold:
                if dry_run_sizes:
                    logger.info(f"    [DRY RUN] Tier 2 (SQL WH): {len(tables_needing_size)} tables - skipped")
                else:
                    logger.info(f"    Tier 2 (SQL WH): {len(tables_needing_size)} tables")
                    self._collect_sizes_parallel(tables_needing_size, warehouse_id)
            else:
                if cluster_id:
                    if dry_run_sizes:
                        logger.info(f"    [DRY RUN] Tier 3 (Spark): {len(tables_needing_size)} tables - skipped")
                    else:
                        logger.info(f"    Tier 3 (Spark): {len(tables_needing_size)} tables")
                        self._collect_sizes_spark_job(tables_needing_size, cluster_id, warehouse_id)
                else:
                    if dry_run_sizes:
                        logger.info(f"    [DRY RUN] Tier 2 fallback: {len(tables_needing_size)} tables - skipped")
                    else:
                        logger.warning(f"    Tier 2 fallback: {len(tables_needing_size)} tables (use --cluster-id for Spark)")
                        self._collect_sizes_parallel(tables_needing_size, warehouse_id)
        
        except Exception as e:
            logger.error(f"Error listing tables in catalog {self.catalog.catalog_name}: {str(e)}")
        
        return all_tables
    
    def _list_tables_in_schema(self, schema_name: str, warehouse_id: str = None, size_map: dict = None) -> List[TableModel]:
        """List tables in a schema and apply pre-fetched sizes."""
        tables = []
        size_map = size_map or {}
        
        try:
            table_list = self.client.tables.list(
                catalog_name=self.catalog.catalog_name,
                schema_name=schema_name
            )
            
            for tbl in table_list:
                properties_dict = dict(tbl.properties) if tbl.properties else {}
                
                # Determine table format from SDK data_source_format first
                table_type_str = tbl.table_type.value if tbl.table_type else "UNKNOWN"
                data_source_fmt = tbl.data_source_format.value if tbl.data_source_format else None
                
                # Parse properties for additional metadata
                parsed_props = parse_table_properties(properties_dict, data_source_fmt, table_type_str)
                
                columns = []
                if self.include_column_details and tbl.columns:
                    for col in tbl.columns:
                        columns.append({
                            'name': col.name,
                            'type': col.type_name.value if col.type_name else col.type_text,
                            'position': col.position,
                            'nullable': col.nullable,
                            'comment': col.comment
                        })
                
                table = TableModel(
                    table_name=tbl.name,
                    schema_name=tbl.schema_name,
                    catalog_name=tbl.catalog_name,
                    table_type=table_type_str,
                    data_source_format=data_source_fmt,
                    storage_location=tbl.storage_location,
                    owner=tbl.owner,
                    comment=tbl.comment,
                    properties=properties_dict,
                    created_at=tbl.created_at,
                    created_by=tbl.created_by,
                    updated_at=tbl.updated_at,
                    updated_by=tbl.updated_by,
                    columns=columns,
                    workspace_id=self.catalog.workspace_id,
                    workspace_name=self.catalog.workspace_name,
                    metastore_id=self.catalog.metastore_id,
                    metastore_name=self.catalog.metastore_name,
                    table_format=parsed_props['table_format'],
                    is_delta=parsed_props['is_delta'],
                    is_iceberg=parsed_props['is_iceberg'],
                    is_uniform=parsed_props['is_uniform']
                )
                
                # Apply pre-fetched size from bulk query if available
                full_name = f"{tbl.catalog_name}.{tbl.schema_name}.{tbl.name}"
                if full_name in size_map:
                    table.size_bytes = size_map[full_name].get('size_bytes')
                    table.num_files = size_map[full_name].get('num_files')
                
                tables.append(table)
        
        except Exception as e:
            logger.error(f"Error listing tables in schema {schema_name}: {str(e)}")
            raise
        
        return tables
    
    def _get_table_sizes_bulk(self, warehouse_id: str) -> dict:
        """
        Get all table sizes in ONE query using system.information_schema.table_storage_info.
        This is much faster than per-table DESCRIBE DETAIL queries.
        
        Returns:
            dict: {full_table_name: {'size_bytes': int, 'num_files': int}}
        """
        if not warehouse_id:
            return {}
        
        logger.info(f"  Getting table sizes (bulk query) for catalog: {self.catalog.catalog_name}")
        
        try:
            from databricks.sdk.service.sql import StatementState
            
            # Try system.information_schema.table_storage_info first (fastest)
            query = f"""
            SELECT 
                table_catalog,
                table_schema,
                table_name,
                CAST(COALESCE(data_size_bytes, 0) AS BIGINT) as size_bytes,
                CAST(COALESCE(num_files, 0) AS INT) as num_files
            FROM system.information_schema.table_storage_info
            WHERE table_catalog = '{self.catalog.catalog_name}'
            """
            
            response = self.client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=query,
                wait_timeout='50s'
            )
            
            size_map = {}
            if response.status.state == StatementState.SUCCEEDED and response.result and response.result.data_array:
                for row in response.result.data_array:
                    catalog, schema, table, size_bytes, num_files = row
                    full_name = f"{catalog}.{schema}.{table}"
                    size_map[full_name] = {
                        'size_bytes': int(size_bytes) if size_bytes else None,
                        'num_files': int(num_files) if num_files else None
                    }
                logger.info(f"    Retrieved sizes for {len(size_map)} tables via system tables")
                return size_map
            
            logger.debug(f"    System table query returned no results, will use fallback")
            return {}
            
        except Exception as e:
            logger.debug(f"    Bulk size query failed: {str(e)[:80]}, will use fallback")
            return {}
    
    def _collect_sizes_parallel(self, tables: List[TableModel], warehouse_id: str):
        """
        TIER 2: Collect table sizes using parallel DESCRIBE DETAIL queries via SQL warehouse.
        Best for < 200 tables.
        """
        if not tables or not warehouse_id:
            return
        
        from databricks.sdk.service.sql import StatementState
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def get_size(table):
            try:
                full_name = f"{table.catalog_name}.{table.schema_name}.{table.table_name}"
                response = self.client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=f"DESCRIBE DETAIL {full_name}",
                    wait_timeout='30s'
                )
                
                if response.status.state == StatementState.SUCCEEDED and response.result and response.result.data_array:
                    cols = [c.name for c in response.manifest.schema.columns]
                    row = response.result.data_array[0]
                    data = dict(zip(cols, row))
                    return table, data.get('sizeInBytes'), data.get('numFiles')
                return table, None, None
            except Exception as e:
                logger.debug(f"      Size query failed for {table.table_name}: {str(e)[:40]}")
                return table, None, None
        
        # Execute queries in parallel (configurable workers)
        collected = 0
        with ThreadPoolExecutor(max_workers=self.size_workers) as executor:
            futures = [executor.submit(get_size, t) for t in tables]
            for future in as_completed(futures):
                table, size_bytes, num_files = future.result()
                if size_bytes is not None:
                    table.size_bytes = int(size_bytes)
                    collected += 1
                if num_files is not None:
                    table.num_files = int(num_files)
        
        logger.info(f"      Collected sizes for {collected}/{len(tables)} tables")
    
    def _collect_sizes_spark_job(self, tables: List[TableModel], cluster_id: str, warehouse_id: str = None):
        """
        TIER 3: Collect table sizes using a Spark job on a cluster.
        Best for >= 200 tables.
        
        For very large catalogs (30k+ tables), splits into chunks and runs multiple jobs.
        Uses dbutils.notebook.exit() to return results (no storage needed).
        """
        # Chunk size for notebook output limit (~5MB / ~130 bytes per table = ~38k, use 30k to be safe)
        CHUNK_SIZE = 30000
        
        if len(tables) > CHUNK_SIZE:
            # Split into chunks and process each
            logger.info(f"      Large catalog: splitting {len(tables)} tables into {(len(tables) // CHUNK_SIZE) + 1} chunks")
            for i in range(0, len(tables), CHUNK_SIZE):
                chunk = tables[i:i + CHUNK_SIZE]
                chunk_num = (i // CHUNK_SIZE) + 1
                logger.info(f"      Processing chunk {chunk_num} ({len(chunk)} tables)")
                self._run_spark_size_job(chunk, cluster_id)
        else:
            self._run_spark_size_job(tables, cluster_id)
    
    def _run_spark_size_job(self, tables: List[TableModel], cluster_id: str):
        """Run a single Spark job to collect sizes for a batch of tables."""
        import time
        import json
        from databricks.sdk.service.jobs import Task, NotebookTask, RunLifeCycleState, RunResultState
        from databricks.sdk.service.workspace import ImportFormat, Language
        import base64
        
        table_names = [f"{t.catalog_name}.{t.schema_name}.{t.table_name}" for t in tables]
        catalog = tables[0].catalog_name if tables else "default"
        
        notebook_content = f'''# Databricks notebook source
# Table Size Collector - Auto-generated (parallel execution)
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

table_names = {table_names}
results = []

def get_size(full_name):
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {{full_name}}")
        row = detail.first()
        return {{
            'table_name': full_name,
            'size_bytes': int(row['sizeInBytes']) if row and row['sizeInBytes'] else None,
            'num_files': int(row['numFiles']) if row and row['numFiles'] else None
        }}
    except:
        return {{'table_name': full_name, 'size_bytes': None, 'num_files': None}}

# Parallel execution (50 workers for speed)
with ThreadPoolExecutor(max_workers=50) as executor:
    futures = {{executor.submit(get_size, t): t for t in table_names}}
    for future in as_completed(futures):
        results.append(future.result())

# Return results directly via notebook output
dbutils.notebook.exit(json.dumps(results))
'''
        
        try:
            # Upload notebook
            notebook_path = "/Workspace/Shared/_metadata_collector_temp"
            self.client.workspace.import_(
                path=notebook_path,
                content=base64.b64encode(notebook_content.encode()).decode(),
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True
            )
            logger.info(f"      Uploaded Spark job notebook")
            
            # Submit job
            run = self.client.jobs.submit(
                run_name=f"Table Size Collection - {catalog}",
                tasks=[
                    Task(
                        task_key="collect_sizes",
                        existing_cluster_id=cluster_id,
                        notebook_task=NotebookTask(notebook_path=notebook_path)
                    )
                ]
            )
            logger.info(f"      Submitted Spark job (run_id: {run.run_id})")
            
            # Wait for completion
            while True:
                run_status = self.client.jobs.get_run(run.run_id)
                state = run_status.state.life_cycle_state
                
                if state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED]:
                    break
                elif state == RunLifeCycleState.INTERNAL_ERROR:
                    error_msg = run_status.state.state_message if run_status.state else "Unknown"
                    logger.error(f"      Spark job internal error: {error_msg}")
                    # Try to get task error
                    if run_status.tasks:
                        for task in run_status.tasks:
                            if task.state and task.state.state_message:
                                logger.error(f"      Task error: {task.state.state_message}")
                    return
                
                time.sleep(5)
            
            # Check result
            if run_status.state.result_state != RunResultState.SUCCESS:
                error_msg = run_status.state.state_message if run_status.state else "Unknown"
                logger.error(f"      Spark job failed: {error_msg}")
                # Log task-level errors
                if run_status.tasks:
                    for task in run_status.tasks:
                        if task.state and task.state.state_message:
                            logger.error(f"      Task error: {task.state.state_message}")
                return
            
            logger.info("      Spark job completed successfully")
            
            # Read results from notebook output (via task output)
            try:
                # Get the task run_id from the completed run
                task_run_id = None
                if run_status.tasks and len(run_status.tasks) > 0:
                    task_run_id = run_status.tasks[0].run_id
                
                if task_run_id:
                    # Get the notebook output from the task
                    task_output = self.client.jobs.get_run_output(task_run_id)
                    results_json = task_output.notebook_output.result if task_output.notebook_output else None
                else:
                    results_json = None
                
                if results_json:
                    results_data = json.loads(results_json)
                    
                    # Build lookup
                    size_lookup = {}
                    for item in results_data:
                        size_lookup[item['table_name']] = {
                            'size_bytes': item.get('size_bytes'),
                            'num_files': item.get('num_files')
                        }
                    
                    # Apply to tables
                    applied = 0
                    for table in tables:
                        full_name = f"{table.catalog_name}.{table.schema_name}.{table.table_name}"
                        if full_name in size_lookup:
                            table.size_bytes = int(size_lookup[full_name]['size_bytes']) if size_lookup[full_name]['size_bytes'] else None
                            table.num_files = int(size_lookup[full_name]['num_files']) if size_lookup[full_name]['num_files'] else None
                            applied += 1
                    
                    logger.info(f"      Applied sizes to {applied}/{len(tables)} tables")
                else:
                    logger.warning("      No notebook output returned")
            except Exception as read_err:
                logger.warning(f"      Could not read Spark job results: {str(read_err)[:80]}")
            
        except Exception as e:
            logger.error(f"      Spark job error: {str(e)[:100]}")
        finally:
            # Cleanup notebook
            try:
                self.client.workspace.delete(notebook_path)
            except:
                pass

    def _get_table_size(self, catalog_name: str, schema_name: str, table_name: str, warehouse_id: str = None) -> dict:
        if not warehouse_id:
            return {}
        
        try:
            from databricks.sdk.service.sql import StatementState
            
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            query = f"DESCRIBE DETAIL {full_table_name}"
            
            response = self.client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=query,
                wait_timeout='10s'
            )
            
            if response.status.state == StatementState.SUCCEEDED and response.result:
                if response.result.data_array and len(response.result.data_array) > 0:
                    row = response.result.data_array[0]
                    
                    col_names = [col.name for col in response.manifest.schema.columns] if response.manifest and response.manifest.schema else []
                    
                    result_dict = dict(zip(col_names, row))
                    
                    size_bytes = result_dict.get('sizeInBytes')
                    num_files = result_dict.get('numFiles')
                    
                    return {
                        'size_bytes': int(size_bytes) if size_bytes else None,
                        'num_files': int(num_files) if num_files else None
                    }
            
            return {}
            
        except Exception as e:
            logger.debug(f"Could not get size for {table_name}: {str(e)}")
            return {}
    
    def list_all_volumes(self) -> List[VolumeModel]:
        logger.debug(f"Listing volumes in catalog: {self.catalog.catalog_name}")
        
        all_volumes = []
        
        try:
            schemas = self.list_schemas()
            
            # Filter excluded schemas
            if self.exclude_schemas:
                schemas = [s for s in schemas if s.schema_name not in self.exclude_schemas]
            
            for schema in schemas:
                try:
                    volumes = self._list_volumes_in_schema(schema.schema_name)
                    all_volumes.extend(volumes)
                except Exception as e:
                    logger.warning(f"Error listing volumes in schema {schema.schema_name}: {str(e)}")
                    continue
        
        except Exception as e:
            logger.error(f"Error listing volumes in catalog {self.catalog.catalog_name}: {str(e)}")
        
        return all_volumes
    
    def _list_volumes_in_schema(self, schema_name: str) -> List[VolumeModel]:
        volumes = []
        
        try:
            volume_list = self.client.volumes.list(
                catalog_name=self.catalog.catalog_name,
                schema_name=schema_name
            )
            
            for vol in volume_list:
                volume = VolumeModel(
                    volume_name=vol.name,
                    schema_name=vol.schema_name,
                    catalog_name=vol.catalog_name,
                    volume_type=vol.volume_type.value if vol.volume_type else "UNKNOWN",
                    storage_location=vol.storage_location,
                    owner=vol.owner,
                    comment=vol.comment,
                    created_at=vol.created_at,
                    created_by=vol.created_by,
                    updated_at=vol.updated_at,
                    updated_by=vol.updated_by,
                    workspace_id=self.catalog.workspace_id,
                    workspace_name=self.catalog.workspace_name,
                    metastore_id=self.catalog.metastore_id,
                    metastore_name=self.catalog.metastore_name
                )
                volumes.append(volume)
        
        except Exception as e:
            logger.error(f"Error listing volumes in schema {schema_name}: {str(e)}")
            raise
        
        return volumes