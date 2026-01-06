#!/usr/bin/env python3
"""
Databricks Metadata Tool - Multi-Cloud Edition

Supports:
- OAuth M2M authentication (DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET) - Recommended
- Azure Service Principal (AZURE_CLIENT_ID + AZURE_TENANT_ID + AZURE_CLIENT_SECRET)
- AWS, Azure, and GCP clouds

Cloud-specific IDs (subscription_id, AWS account, GCP project) are extracted
from the Databricks Account API - no cloud provider APIs required.
"""

import sys
import os
import argparse
from pathlib import Path

from src.utils import setup_logging, load_config
from src.orchestrator import MetadataOrchestrator


def main():
    parser = argparse.ArgumentParser(
        description='Databricks Metadata Tool - Multi-Cloud Account-level metadata collection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Multi-Cloud Authentication:
  Option 1 (Recommended): OAuth M2M - Works across Azure, AWS, GCP
    export DATABRICKS_CLIENT_ID=<service-principal-app-id>
    export DATABRICKS_CLIENT_SECRET=<service-principal-secret>
    export DATABRICKS_ACCOUNT_ID=<account-id>
  
  Option 2: Azure Service Principal - Azure only
    export AZURE_CLIENT_ID=<sp-app-id>
    export AZURE_TENANT_ID=<tenant-id>
    export AZURE_CLIENT_SECRET=<sp-secret>
    export DATABRICKS_ACCOUNT_ID=<account-id>

Modes:

  SCAN (Discovery only)
    python main.py --scan                    # Discover workspaces and metastores
    python main.py --scan --deep-scan        # Include workspace connections

  COLLECT (Full collection - includes scan)
    python main.py --collect --admin-workspace <url> --warehouse-id <id>
    python main.py --collect-dryrun --admin-workspace <url> --warehouse-id <id>

Output Files:
  account_*     - Account-level: workspaces, metastores, summary
  collect_*     - Metastore-level: catalogs, tables, schemas, volumes
        '''
    )
    
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        '--scan',
        action='store_true',
        help='Scan account for workspaces and metastores (discovery only)'
    )
    mode_group.add_argument(
        '--collect',
        action='store_true',
        help='Full collection: scan + catalog/table metadata with sizes'
    )
    mode_group.add_argument(
        '--collect-dryrun',
        action='store_true',
        dest='collect_dryrun',
        help='Full collection without size queries (tier selection logged)'
    )
    
    parser.add_argument(
        '--cloud',
        type=str,
        choices=['azure', 'aws', 'gcp'],
        default='azure',
        help='Cloud provider (azure, aws, gcp) - determines Account API host'
    )
    
    parser.add_argument(
        '--discovery',
        type=str,
        choices=['account', 'azure'],
        default='account',
        help='Discovery mode: account (Databricks Account API) or azure (Azure Management API)'
    )
    
    parser.add_argument(
        '--test-workspace',
        type=str,
        help='[Phase 1] Test scan on a single workspace by name (for testing)'
    )
    
    parser.add_argument(
        '--deep-scan',
        action='store_true',
        help='[Phase 1] Deep scan: verify metastore assignments by connecting to each workspace (slower)'
    )
    
    # Note: quick-scan is the default, so no explicit flag needed
    # Users use --deep-scan to enable workspace verification
    
    parser.add_argument(
        '--metastore-id',
        type=str,
        help='[Phase 2] Specific metastore to collect from (optional)'
    )
    
    parser.add_argument(
        '--admin-workspace',
        type=str,
        help='[Phase 2] Admin workspace name for metastore-level collection'
    )
    
    parser.add_argument(
        '--warehouse-id',
        type=str,
        help='[Phase 2] SQL Warehouse ID for table size collection'
    )
    
    parser.add_argument(
        '--size-workers',
        type=int,
        default=20,
        help='[Phase 2] Number of parallel workers for size collection (default: 20)'
    )
    
    parser.add_argument(
        '--cluster-id',
        type=str,
        help='[Phase 2] Cluster ID for Spark job fallback (used when > 200 tables)'
    )
    
    parser.add_argument(
        '--size-threshold',
        type=int,
        default=200,
        help='[Phase 2] Table count threshold for Spark job (default: 200)'
    )
    
    # Volume output options
    parser.add_argument(
        '--write-to-volume',
        action='store_true',
        help='Upload files to Unity Catalog volume (creates collection_catalog.collection_schema.collection_volume)'
    )
    
    parser.add_argument(
        '--volume-catalog',
        type=str,
        default='collection_catalog',
        help='Catalog name for volume output (default: collection_catalog)'
    )
    
    parser.add_argument(
        '--volume-schema',
        type=str,
        default='collection_schema',
        help='Schema name for volume output (default: collection_schema)'
    )
    
    parser.add_argument(
        '--volume-name',
        type=str,
        default='collection_volume',
        help='Volume name for output files (default: collection_volume)'
    )
    
    # Delta output options (CSVs are always exported by default)
    parser.add_argument(
        '--write-delta',
        action='store_true',
        help='Write results to Delta tables in a Databricks workspace'
    )
    
    parser.add_argument(
        '--delta-workspace',
        type=str,
        help='Workspace URL for Delta output (e.g., https://adb-xxx.azuredatabricks.net)'
    )
    
    parser.add_argument(
        '--delta-catalog',
        type=str,
        default=None,
        help='Catalog for Delta tables (overrides config)'
    )
    
    parser.add_argument(
        '--delta-schema',
        type=str,
        default=None,
        help='Schema for Delta tables (overrides config)'
    )
    
    parser.add_argument(
        '--delta-warehouse-id',
        type=str,
        help='Warehouse ID for Delta table writes'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (overrides config)'
    )
    
    parser.add_argument(
        '--output-format',
        type=str,
        choices=['json', 'yaml'],
        help='Output format (overrides config)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Output directory (overrides config)'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        help='Maximum parallel workers (overrides config)'
    )
    
    parser.add_argument(
        '--resource-group',
        type=str,
        help='Filter by specific Azure resource group (overrides config)'
    )
    
    parser.add_argument(
        '--workspace',
        type=str,
        help='[Legacy] Scan only a specific workspace by name'
    )
    
    parser.add_argument(
        '--exclude-workspace',
        type=str,
        action='append',
        dest='exclude_workspaces',
        help='Exclude specific workspace(s) by name (can be used multiple times)'
    )
    
    parser.add_argument(
        '--exclude-catalog',
        type=str,
        action='append',
        dest='exclude_catalogs',
        help='Exclude specific catalog(s) by name (can be used multiple times)'
    )
    
    parser.add_argument(
        '--exclude-schema',
        type=str,
        action='append',
        dest='exclude_schemas',
        help='Exclude specific schema(s) by name (can be used multiple times)'
    )
    
    args = parser.parse_args()
    
    if args.collect or args.collect_dryrun:
        if not args.admin_workspace:
            parser.error("--collect/--collect-dryrun requires --admin-workspace")
        if not args.warehouse_id:
            parser.error("--collect/--collect-dryrun requires --warehouse-id")
    
    try:
        print(f"Loading configuration from: {args.config}")
        config = load_config(args.config)
        
        if args.log_level:
            config['logging']['level'] = args.log_level
        
        if args.output_format:
            config['output']['format'] = args.output_format
        
        if args.output_dir:
            config['output']['directory'] = args.output_dir
        
        if args.max_workers:
            config['collection']['max_workers'] = args.max_workers
        
        if args.resource_group:
            config['azure']['resource_group'] = args.resource_group
        
        if args.workspace:
            config['azure']['workspace_filter'] = args.workspace
        
        if args.exclude_workspaces:
            existing = config['azure'].get('exclude_workspaces', [])
            config['azure']['exclude_workspaces'] = list(set(existing + args.exclude_workspaces))
        
        if args.exclude_catalogs:
            existing = config['azure'].get('exclude_catalogs', [])
            config['azure']['exclude_catalogs'] = list(set(existing + args.exclude_catalogs))
        
        if args.exclude_schemas:
            existing = config['azure'].get('exclude_schemas', [])
            config['azure']['exclude_schemas'] = list(set(existing + args.exclude_schemas))
        
        if args.scan:
            config['execution_mode'] = 'scan'
            config['discovery_mode'] = args.discovery
            config['deep_scan'] = args.deep_scan
            config['databricks']['cloud'] = args.cloud
            if args.test_workspace:
                config['test_workspace'] = args.test_workspace
        elif args.collect or args.collect_dryrun:
            config['databricks']['cloud'] = args.cloud
            config['execution_mode'] = 'collect'
            config['admin_workspace'] = args.admin_workspace
            config['collection']['warehouse_id'] = args.warehouse_id
            config['dry_run_sizes'] = args.collect_dryrun  # Set dry run mode
            if args.metastore_id:
                config['metastore_id'] = args.metastore_id
        else:
            config['execution_mode'] = 'legacy'
        
        log_level = config.get('logging', {}).get('level', 'INFO')
        log_file = config.get('logging', {}).get('file')
        logger = setup_logging(log_level, log_file)
        
        mode_name = config['execution_mode'].upper()
        logger.info(f"Databricks Metadata Tool starting... (Mode: {mode_name})")
        logger.info(f"Configuration loaded from: {args.config}")
        
        # Validate credentials - check OAuth M2M first, then Azure SP
        discovery_mode = config.get('discovery_mode', 'account')
        
        # Check for OAuth M2M credentials (preferred - multi-cloud)
        has_oauth_m2m = (
            os.getenv('DATABRICKS_CLIENT_ID') and 
            os.getenv('DATABRICKS_CLIENT_SECRET')
        )
        
        # Check for Azure SP credentials (Azure only)
        has_azure_sp = (
            os.getenv('AZURE_CLIENT_ID') and 
            os.getenv('AZURE_CLIENT_SECRET')
        )
        
        if discovery_mode == 'account':
            databricks_config = config.get('databricks', {})
            if not databricks_config.get('account_id'):
                logger.error("Missing DATABRICKS_ACCOUNT_ID for account-level discovery")
                logger.error("Set it in .env or config.yaml")
                sys.exit(1)
            
            if has_oauth_m2m:
                logger.info("Using OAuth M2M authentication (multi-cloud)")
            elif has_azure_sp:
                logger.info("Using Azure Service Principal authentication")
            else:
                logger.error("No valid credentials found. Set one of:")
                logger.error("  1. DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (OAuth M2M)")
                logger.error("  2. AZURE_CLIENT_ID + AZURE_TENANT_ID + AZURE_CLIENT_SECRET (Azure SP)")
                sys.exit(1)
        else:
            # Azure discovery requires Azure SP
            if not has_azure_sp:
                logger.error("Azure discovery mode requires Azure Service Principal credentials")
                logger.error("Set: AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET")
                sys.exit(1)
            
            azure_config = config.get('azure', {})
            if not azure_config.get('subscription_id'):
                logger.error("Azure discovery mode requires AZURE_SUBSCRIPTION_ID")
                sys.exit(1)
        
        orchestrator = MetadataOrchestrator(config)
        
        if config['execution_mode'] == 'scan':
            discovery_mode = config.get('discovery_mode', 'account')
            test_workspace = config.get('test_workspace')
            deep_scan = config.get('deep_scan', False)
            result = orchestrator.scan_workspaces(discovery_mode=discovery_mode, test_workspace=test_workspace, deep_scan=deep_scan)
        elif config['execution_mode'] == 'collect':
            result = orchestrator.collect_from_admin(
                args.admin_workspace, 
                args.warehouse_id,
                size_workers=getattr(args, 'size_workers', 20),
                cluster_id=getattr(args, 'cluster_id', None),
                size_threshold=getattr(args, 'size_threshold', 200),
                dry_run_sizes=config.get('dry_run_sizes', False)
            )
        else:
            result = orchestrator.collect_all()
        
        # Write to Delta tables if requested (via CLI or config)
        # Note: CSVs are always exported by the orchestrator
        delta_config = config.get('delta_output', {})
        write_delta = args.write_delta or delta_config.get('enabled', False)
        
        if write_delta:
            from src.delta_writer import DeltaWriter
            
            # CLI args override config values
            delta_workspace = args.delta_workspace or delta_config.get('workspace_url')
            delta_catalog = args.delta_catalog or delta_config.get('catalog', 'metadata_collection')
            delta_schema = args.delta_schema or delta_config.get('schema', 'discovery')
            delta_warehouse = args.delta_warehouse_id or delta_config.get('warehouse_id')
            
            if not delta_workspace:
                logger.error("Delta output requires workspace_url (--delta-workspace or config)")
                sys.exit(1)
            if not delta_warehouse:
                logger.error("Delta output requires warehouse_id (--delta-warehouse-id or config)")
                sys.exit(1)
            
            logger.info("")
            logger.info("Writing to Delta tables...")
            
            delta_writer = DeltaWriter(
                workspace_url=delta_workspace,
                catalog=delta_catalog,
                schema=delta_schema,
                warehouse_id=delta_warehouse
            )
            
            result_dict = result.to_dict()
            if config['execution_mode'] == 'scan':
                delta_writer.write_scan_results(result_dict)
            else:
                delta_writer.write_collection_results(result_dict)
        
        # Write to Unity Catalog volume if requested
        if args.write_to_volume and config['execution_mode'] in ('collect', 'collect_dryrun'):
            from src.volume_writer import VolumeWriter
            
            output_dir = config.get('output', {}).get('directory', './outputs')
            
            logger.info("")
            logger.info("Uploading files to Unity Catalog volume...")
            
            volume_writer = VolumeWriter(
                workspace_url=args.admin_workspace,
                catalog=args.volume_catalog,
                schema=args.volume_schema,
                volume=args.volume_name
            )
            
            upload_result = volume_writer.upload_files(output_dir)
            logger.info(f"Volume: {upload_result['volume_full_name']}")
            logger.info(f"Path: {upload_result['volume_path']}")
        
        if result.errors:
            logger.warning(f"Completed with {len(result.errors)} error(s)")
            sys.exit(1)
        else:
            logger.info(f"{mode_name} completed successfully!")
            sys.exit(0)
    
    except FileNotFoundError as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        print("\nPlease create a config.yaml file. You can copy config.yaml.example:", file=sys.stderr)
        print("  cp config.yaml.example config.yaml", file=sys.stderr)
        sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n\nOperation interrupted by user", file=sys.stderr)
        sys.exit(130)
    
    except Exception as e:
        print(f"Fatal error: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
