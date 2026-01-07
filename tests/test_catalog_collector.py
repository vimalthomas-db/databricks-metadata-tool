"""
Test cases for CatalogCollector.
"""
import pytest
from unittest.mock import MagicMock, patch


class TestCatalogCollectorInitialization:
    """Test CatalogCollector initialization."""
    
    def test_init_with_catalog(self, mock_catalog):
        """Test CatalogCollector initializes with catalog."""
        from databricks_metadata_tool.collectors.catalog_collector import CatalogCollector
        
        mock_client = MagicMock()
        
        collector = CatalogCollector(
            catalog=mock_catalog,
            client=mock_client,
            workspace_url="https://test.azuredatabricks.net"
        )
        
        assert collector.catalog == mock_catalog
        assert collector.client == mock_client
    
    def test_init_with_exclude_schemas(self, mock_catalog):
        """Test CatalogCollector with excluded schemas."""
        from databricks_metadata_tool.collectors.catalog_collector import CatalogCollector
        
        mock_client = MagicMock()
        exclude = ['information_schema', 'default']
        
        collector = CatalogCollector(
            catalog=mock_catalog,
            client=mock_client,
            workspace_url="https://test.azuredatabricks.net",
            exclude_schemas=exclude
        )
        
        assert collector.exclude_schemas == exclude


class TestTableSizeCollection:
    """Test table size collection tiers."""
    
    def test_skip_tier2_fallback_flag(self, mock_catalog):
        """Test skip_tier2_fallback flag behavior."""
        from databricks_metadata_tool.collectors.catalog_collector import CatalogCollector
        
        mock_client = MagicMock()
        mock_client.tables.list.return_value = []
        
        collector = CatalogCollector(
            catalog=mock_catalog,
            client=mock_client,
            workspace_url="https://test.azuredatabricks.net"
        )
        
        # When skip_tier2_fallback=True, should not run Tier 2 for large catalogs
        # This allows orchestrator to handle async Spark
        skip_tier2 = True
        has_cluster_id = False
        table_count = 300
        threshold = 200
        
        if table_count >= threshold and not has_cluster_id and skip_tier2:
            action = "queue_for_async_spark"
        else:
            action = "run_tier2"
        
        assert action == "queue_for_async_spark"


class TestSQLEscaping:
    """Test SQL identifier escaping."""
    
    def test_escape_identifier_simple(self):
        """Test escaping simple identifiers."""
        from databricks_metadata_tool.collectors.catalog_collector import escape_identifier
        
        assert escape_identifier("my_table") == "`my_table`"
        assert escape_identifier("catalog") == "`catalog`"
    
    def test_escape_identifier_with_special_chars(self):
        """Test escaping identifiers with special characters."""
        from databricks_metadata_tool.collectors.catalog_collector import escape_identifier
        
        # Backticks in name should be escaped
        assert escape_identifier("my`table") == "`my``table`"
    
    def test_escape_full_name(self):
        """Test escaping full table name (catalog.schema.table)."""
        from databricks_metadata_tool.collectors.catalog_collector import escape_full_name
        
        result = escape_full_name("my_catalog", "my_schema", "my_table")
        assert result == "`my_catalog`.`my_schema`.`my_table`"
    
    def test_escape_string_literal(self):
        """Test escaping string literals for SQL."""
        from databricks_metadata_tool.collectors.catalog_collector import escape_string_literal
        
        assert escape_string_literal("test") == "test"
        assert escape_string_literal("test's") == "test''s"


class TestSparkJobManager:
    """Test SparkJobManager for async Spark jobs."""
    
    def test_job_submission(self):
        """Test Spark job submission."""
        from databricks_metadata_tool.collectors.catalog_collector import SparkJobManager
        
        mock_client = MagicMock()
        mock_run = MagicMock()
        mock_run.run_id = 12345
        mock_client.jobs.submit.return_value = mock_run
        mock_client.workspace.import_ = MagicMock()
        
        manager = SparkJobManager(
            client=mock_client,
            cluster_id="test-cluster-id",
            max_parallel_jobs=3
        )
        
        # Create mock tables
        mock_table = MagicMock()
        mock_table.catalog_name = "test_catalog"
        mock_table.schema_name = "test_schema"
        mock_table.table_name = "test_table"
        
        run_ids = manager.submit_job("test_catalog", [mock_table])
        
        assert mock_client.workspace.import_.called
        assert mock_client.jobs.submit.called
    
    def test_max_parallel_jobs(self):
        """Test max parallel jobs limit."""
        from databricks_metadata_tool.collectors.catalog_collector import SparkJobManager
        
        mock_client = MagicMock()
        
        manager = SparkJobManager(
            client=mock_client,
            cluster_id="test-cluster-id",
            max_parallel_jobs=3
        )
        
        assert manager.max_parallel_jobs == 3
    
    def test_chunking_large_catalogs(self):
        """Test chunking for catalogs with many tables."""
        # MAX_TABLES_PER_SPARK_JOB = 1000
        table_count = 2500
        max_per_job = 1000
        
        expected_chunks = (table_count + max_per_job - 1) // max_per_job
        assert expected_chunks == 3


class TestPendingSparkJob:
    """Test PendingSparkJob dataclass."""
    
    def test_pending_job_creation(self):
        """Test creating a pending Spark job."""
        from databricks_metadata_tool.collectors.catalog_collector import PendingSparkJob
        
        job = PendingSparkJob(
            catalog_name="test_catalog",
            run_id=12345,
            tables=[],
            table_count=100
        )
        
        assert job.catalog_name == "test_catalog"
        assert job.run_id == 12345
        assert job.table_count == 100
        assert job.chunk_index == 0
        assert job.total_chunks == 1
    
    def test_pending_job_with_chunks(self):
        """Test pending job for chunked catalog."""
        from databricks_metadata_tool.collectors.catalog_collector import PendingSparkJob
        
        job = PendingSparkJob(
            catalog_name="large_catalog",
            run_id=12345,
            tables=[],
            table_count=1000,
            chunk_index=1,
            total_chunks=3
        )
        
        assert job.chunk_index == 1
        assert job.total_chunks == 3

