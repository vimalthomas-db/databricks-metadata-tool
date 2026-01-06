#!/usr/bin/env python3
"""
Deploy the metadata tool to a Databricks workspace volume.

Usage:
    python deploy_to_workspace.py --workspace-url <url>
"""

import os
import argparse
from pathlib import Path
from databricks.sdk import WorkspaceClient

def main():
    parser = argparse.ArgumentParser(description='Deploy metadata tool to Databricks workspace')
    parser.add_argument('--workspace-url', required=True, help='Workspace URL')
    parser.add_argument('--catalog', default='collection_catalog', help='Target catalog')
    parser.add_argument('--schema', default='collection_schema', help='Target schema')
    parser.add_argument('--volume', default='collection_volume', help='Target volume')
    args = parser.parse_args()
    
    client = WorkspaceClient(host=args.workspace_url)
    
    # Volume path for the tool
    tool_path = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}/tool"
    
    # Files to upload
    files_to_upload = [
        'main.py',
        'config.yaml',
        'requirements.txt',
        'src/__init__.py',
        'src/models.py',
        'src/utils.py',
        'src/orchestrator.py',
        'src/scan_exporter.py',
        'src/volume_writer.py',
        'src/delta_writer.py',
        'src/cloud_provider/__init__.py',
        'src/cloud_provider/account_provider.py',
        'src/cloud_provider/azure_provider.py',
        'src/collectors/__init__.py',
        'src/collectors/workspace_collector.py',
        'src/collectors/catalog_collector.py',
    ]
    
    project_root = Path(__file__).parent
    
    print(f"Deploying to: {tool_path}")
    
    for file_path in files_to_upload:
        local_path = project_root / file_path
        if not local_path.exists():
            print(f"  Skipping (not found): {file_path}")
            continue
            
        remote_path = f"{tool_path}/{file_path}"
        
        # Create parent directories by uploading files
        with open(local_path, 'rb') as f:
            content = f.read()
        
        try:
            client.files.upload(remote_path, content, overwrite=True)
            print(f"  Uploaded: {file_path}")
        except Exception as e:
            print(f"  Error uploading {file_path}: {e}")
    
    print()
    print("=" * 60)
    print("Deployment complete!")
    print("=" * 60)
    print()
    print("To run in a Databricks notebook:")
    print()
    print(f"""
# Cell 1: Install dependencies
%pip install databricks-sdk python-dotenv pyyaml requests

# Cell 2: Add tool to path
import sys
sys.path.insert(0, '{tool_path}')

# Cell 3: Run collection
from src.orchestrator import MetadataOrchestrator
from src.utils import load_config

config = load_config('{tool_path}/config.yaml')
config['databricks']['account_id'] = spark.conf.get("spark.databricks.accountId", "")
config['output']['directory'] = '/Volumes/{args.catalog}/{args.schema}/{args.volume}/staging'

orchestrator = MetadataOrchestrator(config)
result = orchestrator.collect_from_admin(
    admin_workspace=f"https://{{spark.conf.get('spark.databricks.workspaceUrl')}}",
    warehouse_id="<your-warehouse-id>",
    dry_run_sizes=True
)
""")

if __name__ == "__main__":
    main()

