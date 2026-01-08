#!/usr/bin/env python3
"""CLI entry point for databricks-metadata-tool."""

import sys
import os
import argparse
import logging

from databricks_metadata_tool.utils import setup_logging, load_config
from databricks_metadata_tool.orchestrator import MetadataOrchestrator

logger = logging.getLogger('databricks_metadata_tool')


def main():
    parser = argparse.ArgumentParser(
        description='Databricks Metadata Tool - Multi-Cloud Account-level metadata collection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  dbmeta --scan                    # Scan account for workspaces
  dbmeta --collect --admin-workspace <url> --warehouse-id <id>
  dbmeta --collect-dryrun --admin-workspace <url> --warehouse-id <id>  # Show tier selection, no files
'''
    )
    
    # Mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--scan', action='store_true', 
                           help='Scan account for workspaces and metastores')
    mode_group.add_argument('--collect', action='store_true', 
                           help='Full collection with table sizes')
    mode_group.add_argument('--collect-dryrun', action='store_true', dest='collect_dryrun', 
                           help='Dry run: show tier selection without collecting sizes')
    
    # Essential arguments
    parser.add_argument('--admin-workspace', type=str, help='Admin workspace URL (required for collect)')
    parser.add_argument('--warehouse-id', type=str, help='SQL Warehouse ID (required for collect)')
    parser.add_argument('--cluster-id', type=str, help='Cluster ID for Spark jobs (large catalogs)')
    parser.add_argument('--only-catalogs', type=str, nargs='+', dest='only_catalogs',
                       help='Only collect tables from these catalogs')
    
    # Output options
    parser.add_argument('--output-dir', type=str, help='Output directory (default: from config)')
    parser.add_argument('--write-to-volume', action='store_true', help='Upload results to Unity Catalog volume')
    
    # Config file
    parser.add_argument('--config', type=str, default='config.yaml', help='Config file path')
    
    args = parser.parse_args()
    
    setup_logging()
    
    # Load config
    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        # If user explicitly specified a config file, fail if not found
        if args.config != 'config.yaml':
            logger.error(f"Config file not found: {args.config}")
            sys.exit(1)
        
        # Default config.yaml not found - use environment variables
        logger.warning("No config.yaml found, using environment variables and defaults")
        config = {
            'databricks': {'account_id': os.getenv('DATABRICKS_ACCOUNT_ID')},
            'collection': {},
            'output': {'directory': './outputs'},
            'filters': {'exclude_catalogs': ['system', 'samples'], 'exclude_schemas': ['information_schema']}
        }
    
    # Ensure sections exist
    if 'collection' not in config:
        config['collection'] = {}
    if 'output' not in config:
        config['output'] = {}
    
    # CLI overrides config
    if args.output_dir:
        config['output']['directory'] = args.output_dir
    
    # Determine mode
    if args.scan:
        config['execution_mode'] = 'scan'
    elif args.collect:
        config['execution_mode'] = 'collect'
    elif args.collect_dryrun:
        config['execution_mode'] = 'collect_dryrun'
        config['dry_run'] = True  # Full dry run: no sizes, no files
    else:
        config['execution_mode'] = 'scan'
    
    # Validate required parameters
    account_id = config.get('databricks', {}).get('account_id') or os.getenv('DATABRICKS_ACCOUNT_ID')
    if not account_id:
        logger.error("DATABRICKS_ACCOUNT_ID not set. Set via environment variable or config.yaml")
        sys.exit(1)
    
    # Validate collect mode parameters
    if config['execution_mode'] in ['collect', 'collect_dryrun']:
        if not args.admin_workspace:
            logger.error("--admin-workspace required for collect mode")
            logger.error("  Example: --admin-workspace https://adb-123.azuredatabricks.net")
            sys.exit(1)
        
        if not args.warehouse_id:
            logger.error("--warehouse-id required for collect mode")
            logger.error("  Example: --warehouse-id abc123def456")
            sys.exit(1)
        
        # Validate workspace URL format
        if not args.admin_workspace.startswith('https://'):
            logger.error(f"Invalid workspace URL: {args.admin_workspace}")
            logger.error("  Must start with https:// (e.g., https://adb-123.azuredatabricks.net)")
            sys.exit(1)
    
    try:
        orchestrator = MetadataOrchestrator(config)
        
        if config['execution_mode'] == 'scan':
            # Scan mode - just discover workspaces and metastores
            result = orchestrator.scan_workspaces(discovery_mode='account')
        else:
            # Collect mode (collect or collect_dryrun)
            collection_cfg = config.get('collection', {})
            
            # Create volume_writer for incremental uploads if requested
            volume_writer = None
            if args.write_to_volume and not config.get('dry_run', False):
                from databricks_metadata_tool.volume_writer import VolumeWriter
                volume_config = config.get('volume', {})
                
                volume_writer = VolumeWriter(
                    workspace_url=args.admin_workspace,
                    catalog=volume_config.get('catalog', 'collection_catalog'),
                    schema=volume_config.get('schema', 'collection_schema'),
                    volume=volume_config.get('volume', 'collection_volume'),
                    staging_folder=volume_config.get('staging_folder', 'staging')
                )
            elif args.write_to_volume and config.get('dry_run', False):
                logger.info("[DRY RUN] Volume upload skipped")
            
            result = orchestrator.collect_from_admin(
                args.admin_workspace,
                args.warehouse_id,
                size_workers=collection_cfg.get('size_workers', 20),
                cluster_id=args.cluster_id,
                size_threshold=collection_cfg.get('size_threshold', 200),
                dry_run=config.get('dry_run', False),
                only_catalogs=args.only_catalogs or collection_cfg.get('only_catalogs'),
                volume_writer=volume_writer
            )
        
        if result.errors:
            logger.warning(f"Completed with {len(result.errors)} error(s)")
            sys.exit(1)
        else:
            logger.info("Completed successfully!")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

