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
    
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--scan', action='store_true', help='Scan account for workspaces and metastores')
    mode_group.add_argument('--collect', action='store_true', help='Full collection with table sizes')
    mode_group.add_argument('--collect-dryrun', action='store_true', dest='collect_dryrun', 
                           help='Dry run: show tier selection without collecting sizes or saving files')
    
    parser.add_argument('--admin-workspace', type=str, help='Admin workspace URL')
    parser.add_argument('--warehouse-id', type=str, help='SQL Warehouse ID')
    parser.add_argument('--config', type=str, default='config.yaml', help='Config file path')
    parser.add_argument('--output-dir', type=str, default='./outputs', help='Output directory')
    parser.add_argument('--write-to-volume', action='store_true', help='Upload to Unity Catalog volume')
    parser.add_argument('--volume-catalog', type=str, help='Volume catalog (default from config.yaml)')
    parser.add_argument('--volume-schema', type=str, help='Volume schema (default from config.yaml)')
    parser.add_argument('--volume-name', type=str, help='Volume name (default from config.yaml)')
    parser.add_argument('--cluster-id', type=str, help='Cluster ID for Spark jobs')
    parser.add_argument('--size-threshold', type=int, default=200, help='Table count threshold for Spark')
    parser.add_argument('--size-workers', type=int, default=20, help='Parallel workers for size collection')
    
    args = parser.parse_args()
    
    setup_logging()
    
    # Load config
    try:
        config = load_config(args.config)
    except FileNotFoundError:
        config = {
            'databricks': {'account_id': os.getenv('DATABRICKS_ACCOUNT_ID')},
            'collection': {},
            'output': {'directory': args.output_dir}
        }
    
    # Override config with CLI args
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
    
    try:
        orchestrator = MetadataOrchestrator(config)
        
        if config['execution_mode'] == 'scan':
            result = orchestrator.scan_workspaces(discovery_mode='account')
        else:
            if not args.admin_workspace or not args.warehouse_id:
                logger.error("--admin-workspace and --warehouse-id required for collect")
                sys.exit(1)
            
            result = orchestrator.collect_from_admin(
                args.admin_workspace,
                args.warehouse_id,
                size_workers=args.size_workers,
                cluster_id=args.cluster_id,
                size_threshold=args.size_threshold,
                dry_run=config.get('dry_run', False)
            )
        
        # Upload to volume if requested (skip in dry-run mode)
        if args.write_to_volume and args.admin_workspace and not config.get('dry_run', False):
            from databricks_metadata_tool.volume_writer import VolumeWriter
            
            # Get volume settings from config, CLI args override
            volume_config = config.get('volume', {})
            vol_catalog = args.volume_catalog or volume_config.get('catalog', 'collection_catalog')
            vol_schema = args.volume_schema or volume_config.get('schema', 'collection_schema')
            vol_name = args.volume_name or volume_config.get('volume', 'collection_volume')
            vol_staging = volume_config.get('staging_folder', 'staging')
            
            logger.info("Uploading to Unity Catalog volume...")
            
            volume_writer = VolumeWriter(
                workspace_url=args.admin_workspace,
                catalog=vol_catalog,
                schema=vol_schema,
                volume=vol_name,
                staging_folder=vol_staging
            )
            
            upload_result = volume_writer.upload_files(args.output_dir)
            logger.info(f"Files uploaded to: {upload_result['volume_path']}")
        elif args.write_to_volume and config.get('dry_run', False):
            logger.info("[DRY RUN] Volume upload skipped")
        
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

