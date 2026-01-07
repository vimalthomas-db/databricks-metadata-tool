"""
Pytest configuration and shared fixtures.
"""
import pytest
import os
import tempfile
import shutil
from unittest.mock import MagicMock, patch


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test outputs."""
    temp_dir = tempfile.mkdtemp(prefix="dbmeta_test_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_config():
    """Sample configuration dictionary."""
    return {
        'databricks': {
            'account_id': 'test-account-id',
            'cloud': 'azure'
        },
        'filters': {
            'exclude_catalogs': ['system', 'samples'],
            'exclude_schemas': ['information_schema'],
            'only_catalogs': None
        },
        'output': {
            'directory': './outputs'
        },
        'volume': {
            'catalog': 'test_catalog',
            'schema': 'test_schema',
            'volume': 'test_volume'
        },
        'collection': {
            'collect_tables': True,
            'collect_volumes': True,
            'collect_sizes': True,
            'size_workers': 20,
            'size_threshold': 200,
            'max_parallel_spark_jobs': 3
        },
        'logging': {
            'level': 'INFO'
        }
    }


@pytest.fixture
def sample_config_with_only_catalogs(sample_config):
    """Config with only_catalogs filter set."""
    config = sample_config.copy()
    config['filters'] = sample_config['filters'].copy()
    config['filters']['only_catalogs'] = ['catalog_a', 'catalog_b']
    return config


@pytest.fixture
def mock_workspace():
    """Mock workspace object."""
    ws = MagicMock()
    ws.workspace_id = "123456789"
    ws.workspace_name = "test-workspace"
    ws.workspace_url = "https://test.azuredatabricks.net"
    ws.location = "eastus"
    ws.cloud = "azure"
    return ws


@pytest.fixture
def mock_catalog():
    """Mock catalog object."""
    from databricks_metadata_tool.models import Catalog
    return Catalog(
        catalog_name="test_catalog",
        catalog_type="MANAGED",
        owner="admin@test.com",
        metastore_id="ms-123",
        metastore_name="test-metastore",
        workspace_id="123456789",
        workspace_name="test-workspace"
    )


@pytest.fixture
def mock_table():
    """Mock table object."""
    from databricks_metadata_tool.models import Table
    return Table(
        table_name="test_table",
        schema_name="default",
        catalog_name="test_catalog",
        table_type="MANAGED",
        data_source_format="DELTA",
        is_delta=True
    )


@pytest.fixture
def mock_account_client():
    """Mock Databricks Account Client."""
    with patch('databricks_metadata_tool.account_provider.AccountClient') as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def mock_workspace_client():
    """Mock Databricks Workspace Client."""
    with patch('databricks.sdk.WorkspaceClient') as mock:
        client = MagicMock()
        mock.return_value = client
        yield client

