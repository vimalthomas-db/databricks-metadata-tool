#!/usr/bin/env python3
"""
Configure Unity Catalog metastore with default storage.

This script:
1. Creates an Azure Access Connector (if not exists)
2. Grants RBAC role on storage account
3. Creates a storage credential in Unity Catalog
4. Sets the metastore default root storage

Prerequisites:
- Azure CLI logged in with permissions to create resources
- DATABRICKS_* environment variables set
"""

import os
import subprocess
import json
import sys

# Configuration
RESOURCE_GROUP = "rg-dbmeta-test"
LOCATION = "eastus2"
STORAGE_ACCOUNT = "stdbmetatestuc"
CONTAINER = "metastore"
ACCESS_CONNECTOR_NAME = "ac-dbmeta-test"
STORAGE_CREDENTIAL_NAME = "metastore-credential"

def run_az(cmd, check=True):
    """Run Azure CLI command and return JSON output."""
    full_cmd = f"az {cmd} --output json"
    print(f"  Running: az {cmd[:60]}...")
    result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"  Error: {result.stderr}")
        return None
    if result.stdout:
        try:
            return json.loads(result.stdout)
        except:
            return result.stdout
    return None

def main():
    print("=" * 60)
    print("Configuring Unity Catalog Metastore Storage")
    print("=" * 60)
    
    # Step 1: Check/Create Access Connector
    print("\n1. Checking Access Connector...")
    ac = run_az(f"databricks access-connector show -n {ACCESS_CONNECTOR_NAME} -g {RESOURCE_GROUP}", check=False)
    
    if not ac:
        print(f"   Creating Access Connector: {ACCESS_CONNECTOR_NAME}")
        ac = run_az(f"databricks access-connector create -n {ACCESS_CONNECTOR_NAME} -g {RESOURCE_GROUP} -l {LOCATION} --identity-type SystemAssigned")
        if not ac:
            print("   Failed to create Access Connector")
            sys.exit(1)
    
    ac_id = ac.get('id')
    ac_principal_id = ac.get('identity', {}).get('principalId')
    print(f"   Access Connector ID: {ac_id}")
    print(f"   Principal ID: {ac_principal_id}")
    
    # Step 2: Grant RBAC on storage account
    print("\n2. Granting Storage Blob Data Contributor role...")
    storage = run_az(f"storage account show -n {STORAGE_ACCOUNT} -g {RESOURCE_GROUP}")
    if not storage:
        print(f"   Storage account {STORAGE_ACCOUNT} not found")
        sys.exit(1)
    
    storage_id = storage.get('id')
    
    # Check if role assignment exists
    role_check = run_az(
        f"role assignment list --assignee {ac_principal_id} --scope {storage_id} --role 'Storage Blob Data Contributor'",
        check=False
    )
    
    if not role_check or len(role_check) == 0:
        run_az(f"role assignment create --assignee {ac_principal_id} --role 'Storage Blob Data Contributor' --scope {storage_id}")
        print("   Role assigned")
    else:
        print("   Role already assigned")
    
    # Step 3: Create storage credential in Unity Catalog
    print("\n3. Creating Storage Credential in Unity Catalog...")
    
    workspace_url = os.getenv('ADMIN_WORKSPACE_URL', 'https://adb-7405611291910857.17.azuredatabricks.net')
    
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import AzureManagedIdentity
    
    client = WorkspaceClient(host=workspace_url)
    
    # Check if credential exists
    try:
        existing = client.storage_credentials.get(STORAGE_CREDENTIAL_NAME)
        print(f"   Storage credential exists: {STORAGE_CREDENTIAL_NAME}")
    except:
        print(f"   Creating storage credential: {STORAGE_CREDENTIAL_NAME}")
        client.storage_credentials.create(
            name=STORAGE_CREDENTIAL_NAME,
            azure_managed_identity=AzureManagedIdentity(
                access_connector_id=ac_id
            ),
            comment="Metastore root storage credential"
        )
        print("   Storage credential created")
    
    # Step 4: Update metastore with default root storage
    print("\n4. Configuring metastore default storage...")
    
    storage_root = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    
    try:
        current_metastore = client.metastores.current()
        metastore_id = current_metastore.metastore_id
        
        print(f"   Metastore ID: {metastore_id}")
        print(f"   Storage root: {storage_root}")
        
        # Get current metastore details
        metastore = client.metastores.get(metastore_id)
        print(f"   Current storage root: {metastore.storage_root or 'None'}")
        
        if metastore.storage_root:
            print("   Metastore already has storage root configured")
        else:
            # Update metastore via Account API
            from databricks.sdk import AccountClient
            
            account_client = AccountClient(
                host="https://accounts.azuredatabricks.net",
                account_id=os.getenv('DATABRICKS_ACCOUNT_ID')
            )
            
            try:
                account_client.metastores.update(
                    metastore_id=metastore_id,
                    storage_root=storage_root,
                    storage_root_credential_id=STORAGE_CREDENTIAL_NAME
                )
                print("   Metastore updated with default storage")
            except Exception as e:
                print(f"   Error: {e}")
                print("   Try updating via Databricks Account Console manually")
        
    except Exception as e:
        print(f"   Error updating metastore: {e}")
        print("   You may need metastore admin privileges")
        sys.exit(1)
    
    # Step 5: Create external location for collection outputs
    print("\n5. Creating External Location...")
    
    try:
        client.external_locations.get("collection-storage")
        print("   External location exists: collection-storage")
    except:
        print("   Creating external location: collection-storage")
        client.external_locations.create(
            name="collection-storage",
            url=storage_root,
            credential_name=STORAGE_CREDENTIAL_NAME,
            comment="Storage for collection outputs"
        )
        print("   External location created")
    
    # Step 6: Create collection catalog
    print("\n6. Creating Collection Catalog...")
    
    COLLECTION_CATALOG = "collection_catalog"
    COLLECTION_SCHEMA = "collection_schema"
    COLLECTION_VOLUME = "collection_volume"
    
    try:
        client.catalogs.get(COLLECTION_CATALOG)
        print(f"   Catalog exists: {COLLECTION_CATALOG}")
    except:
        print(f"   Creating catalog: {COLLECTION_CATALOG}")
        client.catalogs.create(
            name=COLLECTION_CATALOG,
            storage_root=storage_root + COLLECTION_CATALOG,
            comment="Metadata collection outputs"
        )
        print(f"   Catalog created: {COLLECTION_CATALOG}")
    
    # Step 7: Create collection schema
    print("\n7. Creating Collection Schema...")
    
    schema_full = f"{COLLECTION_CATALOG}.{COLLECTION_SCHEMA}"
    try:
        client.schemas.get(schema_full)
        print(f"   Schema exists: {schema_full}")
    except:
        print(f"   Creating schema: {schema_full}")
        client.schemas.create(
            catalog_name=COLLECTION_CATALOG,
            name=COLLECTION_SCHEMA,
            comment="Collection output schema"
        )
        print(f"   Schema created: {schema_full}")
    
    # Step 8: Create collection volume
    print("\n8. Creating Collection Volume...")
    
    from databricks.sdk.service.catalog import VolumeType
    
    volume_full = f"{COLLECTION_CATALOG}.{COLLECTION_SCHEMA}.{COLLECTION_VOLUME}"
    try:
        client.volumes.read(volume_full)
        print(f"   Volume exists: {volume_full}")
    except:
        print(f"   Creating volume: {volume_full}")
        client.volumes.create(
            catalog_name=COLLECTION_CATALOG,
            schema_name=COLLECTION_SCHEMA,
            name=COLLECTION_VOLUME,
            volume_type=VolumeType.MANAGED,
            comment="Metadata collection output files"
        )
        print(f"   Volume created: {volume_full}")
    
    print("\n" + "=" * 60)
    print("Configuration complete!")
    print("=" * 60)
    print(f"\nResources created:")
    print(f"  Storage Account:     {STORAGE_ACCOUNT}")
    print(f"  Access Connector:    {ACCESS_CONNECTOR_NAME}")
    print(f"  Storage Credential:  {STORAGE_CREDENTIAL_NAME}")
    print(f"  External Location:   collection-storage")
    print(f"  Catalog:             {COLLECTION_CATALOG}")
    print(f"  Schema:              {schema_full}")
    print(f"  Volume:              {volume_full}")
    print(f"\nVolume path: /Volumes/{COLLECTION_CATALOG}/{COLLECTION_SCHEMA}/{COLLECTION_VOLUME}/")
    print("\nYou can now run collection with --write-to-volume")

if __name__ == "__main__":
    main()
