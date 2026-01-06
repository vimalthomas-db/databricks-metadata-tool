"""Data models for Databricks metadata collection"""

from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum


class TableType(Enum):
    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"



class TableFormat(Enum):
    DELTA = "DELTA"
    PARQUET = "PARQUET"
    ICEBERG = "ICEBERG"
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    ORC = "ORC"
    TEXT = "TEXT"
    UNKNOWN = "UNKNOWN"

@dataclass
class Workspace:
    """
    Multi-cloud Databricks workspace model.
    
    Supports Azure, AWS, and GCP workspaces with cloud-specific fields.
    Cloud info is extracted from Account API (no cloud provider APIs needed).
    """
    workspace_id: str
    workspace_name: str
    workspace_url: str
    
    # Multi-cloud fields (from Account API)
    cloud: str = 'unknown'  # azure, aws, gcp
    cloud_region: Optional[str] = None
    workspace_status: Optional[str] = None  # RUNNING, FAILED, etc.
    
    # Azure-specific (from Account API azure_workspace_info)
    subscription_id: Optional[str] = None
    resource_group: Optional[str] = None
    
    # AWS-specific (from Account API)
    aws_account_id: Optional[str] = None  # Derived from credentials_id
    credentials_id: Optional[str] = None
    storage_configuration_id: Optional[str] = None
    
    # GCP-specific (from Account API)
    gcp_project_id: Optional[str] = None
    
    # Common fields
    location: Optional[str] = None
    sku: Optional[str] = None  # pricing_tier
    workspace_type: Optional[str] = None  # UNITY_CATALOG, HIVE_METASTORE, INACCESSIBLE
    has_unity_catalog: bool = False
    metastore_id: Optional[str] = None
    metastore_name: Optional[str] = None
    
    # Security & access
    admin_user_count: int = 0
    admin_group_count: int = 0
    admin_users: List[str] = field(default_factory=list)
    admin_groups: List[str] = field(default_factory=list)
    ip_access_list_enabled: bool = False
    ip_access_list_count: int = 0
    serverless_private_endpoint_count: int = 0
    private_access_enabled: bool = False
    
    # CMK fields (from cloud enrichment, may be empty without cloud APIs)
    cmk_managed_services_enabled: bool = False
    cmk_managed_disk_enabled: bool = False
    cmk_dbfs_enabled: bool = False
    cmk_key_vault_uri: Optional[str] = None
    cmk_key_name: Optional[str] = None
    
    # Workspace Features (dynamic - all settings from workspace-conf API)
    # Stored as dict for flexibility - new features auto-included
    workspace_features: Dict[str, Any] = field(default_factory=dict)
    
    # Legacy feature fields (for backward compatibility with CSV schema)
    serverless_compute_enabled: bool = False
    model_serving_enabled: bool = False
    tokens_enabled: bool = False
    max_token_lifetime_days: Optional[int] = None
    results_downloading_enabled: bool = False
    web_terminal_enabled: bool = False
    user_isolation_enabled: bool = False
    
    # Metadata
    region: Optional[str] = None
    collected_at: Optional[str] = None

    def __post_init__(self):
        if not self.region:
            self.region = self.location or self.cloud_region

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
@dataclass
class Catalog:
    catalog_name: str
    catalog_type: str
    owner: Optional[str] = None
    comment: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)
    created_at: Optional[int] = None
    created_by: Optional[str] = None
    updated_at: Optional[int] = None
    updated_by: Optional[str] = None
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None
    metastore_id: Optional[str] = None
    metastore_name: Optional[str] = None
    is_legacy_hms: bool = False
    region: Optional[str] = None
    collected_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Schema:
    schema_name: str
    catalog_name: str
    owner: Optional[str] = None
    comment: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)
    created_at: Optional[int] = None
    created_by: Optional[str] = None
    updated_at: Optional[int] = None
    updated_by: Optional[str] = None
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
@dataclass
class Table:
    table_name: str
    schema_name: str
    catalog_name: str
    table_type: TableType
    data_source_format: Optional[str] = None
    storage_location: Optional[str] = None
    owner: Optional[str] = None
    comment: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)

    created_at: Optional[int] = None
    created_by: Optional[str] = None
    updated_at: Optional[int] = None
    updated_by: Optional[str] = None

    size_bytes: Optional[int] = None
    num_files: Optional[int] = None
    columns: List[Dict[str, Any]] = field(default_factory=list)
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None
    metastore_id: Optional[str] = None
    metastore_name: Optional[str] = None
    table_format: Optional[TableFormat] = None
    is_delta: bool = False
    is_iceberg: bool = False
    is_uniform: bool = False
    region: Optional[str] = None
    collected_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.table_name}"


@dataclass
class Volume:
    volume_name: str
    schema_name: str
    catalog_name: str
    volume_type: str
    storage_location: Optional[str] = None
    owner: Optional[str] = None
    comment: Optional[str] = None
    size_bytes: Optional[int] = None
    created_at: Optional[int] = None
    created_by: Optional[str] = None
    updated_at: Optional[int] = None
    updated_by: Optional[str] = None
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None
    metastore_id: Optional[str] = None
    metastore_name: Optional[str] = None
    region: Optional[str] = None
    collected_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.volume_name}"
        

@dataclass
class CatalogBinding:
    catalog_name: str
    workspace_id: str
    binding_type: str
    metastore_id: Optional[str] = None
    metastore_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ExternalLocation:
    location_name: str
    url: str
    credential_name: Optional[str] = None
    owner: Optional[str] = None
    comment: Optional[str] = None
    read_only: bool = False
    created_at: Optional[int] = None
    created_by: Optional[str] = None
    updated_at: Optional[int] = None
    updated_by: Optional[str] = None
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None
    metastore_id: Optional[str] = None
    metastore_name: Optional[str] = None
    storage_account: Optional[str] = None
    storage_type: Optional[str] = None
    region: Optional[str] = None
    collected_at: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SharedObject:
    name: str
    data_object_type: str
    status: Optional[str] = None
    added_at: Optional[int] = None
    added_by: Optional[str] = None
    cdf_enabled: bool = False
    history_data_sharing_status: Optional[str] = None
    start_version: Optional[int] = None
    shared_as: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ShareRecipient:
    recipient_name: str
    authentication_type: Optional[str] = None
    data_recipient_global_metastore_id: Optional[str] = None
    cloud: Optional[str] = None
    region: Optional[str] = None
    created_at: Optional[int] = None
    created_by: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Share:
    share_name: str
    owner: Optional[str] = None
    comment: Optional[str] = None
    created_at: Optional[int] = None
    created_by: Optional[str] = None
    updated_at: Optional[int] = None
    updated_by: Optional[str] = None
    objects: List[SharedObject] = field(default_factory=list)
    object_count: int = 0
    table_count: int = 0
    schema_count: int = 0
    recipients: List[str] = field(default_factory=list)
    recipient_count: int = 0
    total_size_bytes: Optional[int] = None
    shared_tables: List[str] = field(default_factory=list)
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None
    metastore_id: Optional[str] = None
    metastore_name: Optional[str] = None
    region: Optional[str] = None
    collected_at: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['objects'] = [obj if isinstance(obj, dict) else obj.to_dict() for obj in self.objects]
        return result


@dataclass
class Repo:
    repo_id: int
    repo_path: str
    url: str
    provider: str
    branch: Optional[str] = None
    head_commit_id: Optional[str] = None
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class WorkspaceConfig:
    config_key: str
    config_value: str
    workspace_id: Optional[str] = None
    workspace_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CollectionResult:
    subscription_id: str
    collection_timestamp: str
    workspaces: List[Workspace] = field(default_factory=list)
    catalogs: List[Catalog] = field(default_factory=list)
    schemas: List[Schema] = field(default_factory=list)
    tables: List[Table] = field(default_factory=list)
    volumes: List[Volume] = field(default_factory=list)
    external_locations: List['ExternalLocation'] = field(default_factory=list)
    catalog_bindings: List['CatalogBinding'] = field(default_factory=list)
    shares: List['Share'] = field(default_factory=list)
    repos: List['Repo'] = field(default_factory=list)
    workspace_configs: List['WorkspaceConfig'] = field(default_factory=list)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'subscription_id': self.subscription_id,
            'collection_timestamp': self.collection_timestamp,
            'summary': {
                'workspaces_count': len(self.workspaces),
                'catalogs_count': len(self.catalogs),
                'schemas_count': len(self.schemas),
                'tables_count': len(self.tables),
                'volumes_count': len(self.volumes),
                'external_locations_count': len(self.external_locations),
                'catalog_bindings_count': len(self.catalog_bindings),
                'shares_count': len(self.shares),
                'repos_count': len(self.repos),
                'workspace_configs_count': len(self.workspace_configs),
                'errors_count': len(self.errors)
            },
            'workspaces': [w.to_dict() for w in self.workspaces],
            'catalogs': [c.to_dict() for c in self.catalogs],
            'schemas': [s.to_dict() for s in self.schemas],
            'tables': [t.to_dict() for t in self.tables],
            'volumes': [v.to_dict() for v in self.volumes],
            'external_locations': [e.to_dict() for e in self.external_locations],
            'catalog_bindings': [b.to_dict() for b in self.catalog_bindings],
            'shares': [s.to_dict() for s in self.shares],
            'repos': [r.to_dict() for r in self.repos],
            'workspace_configs': [c.to_dict() for c in self.workspace_configs],
            'errors': self.errors
        }


@dataclass
class MetastoreInfo:
    metastore_id: str
    metastore_name: Optional[str] = None
    region: Optional[str] = None
    workspace_count: int = 0
    workspaces: List[str] = field(default_factory=list)
    suggested_admin_workspace: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ScanResult:
    subscription_id: str
    scan_timestamp: str
    workspaces: List[Workspace] = field(default_factory=list)
    metastores: List[MetastoreInfo] = field(default_factory=list)
    hms_workspaces: List[str] = field(default_factory=list)
    inaccessible_workspaces: List[str] = field(default_factory=list)
    total_workspaces: int = 0
    uc_workspaces: int = 0
    hms_only_workspaces: int = 0
    inaccessible_count: int = 0
    total_metastores: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'subscription_id': self.subscription_id,
            'scan_timestamp': self.scan_timestamp,
            'summary': {
                'total_workspaces': self.total_workspaces,
                'uc_workspaces': self.uc_workspaces,
                'hms_only_workspaces': self.hms_only_workspaces,
                'inaccessible_count': self.inaccessible_count,
                'total_metastores': self.total_metastores,
                'errors_count': len(self.errors)
            },
            'metastores': [m.to_dict() for m in self.metastores],
            'hms_workspaces': self.hms_workspaces,
            'inaccessible_workspaces': self.inaccessible_workspaces,
            'workspaces': [w.to_dict() for w in self.workspaces],
            'errors': self.errors
        }
