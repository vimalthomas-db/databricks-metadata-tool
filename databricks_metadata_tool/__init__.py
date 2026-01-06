"""Databricks Metadata Tool - Multi-cloud account metadata collection."""

__version__ = "1.0.0"

from .orchestrator import MetadataOrchestrator
from databricks_metadata_tool.models import CollectionResult, ScanResult

__all__ = ["MetadataOrchestrator", "CollectionResult", "ScanResult", "__version__"]

