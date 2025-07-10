import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from hdfs_cost_advisor.hdfs.client import HDFSClient, HDFSConfig
from hdfs_cost_advisor.hdfs.analyzer import HDFSMetadataAnalyzer

class TestHDFSClient:
    
    @pytest.fixture
    def hdfs_config(self):
        """Test HDFS configuration"""
        return HDFSConfig(
            host="localhost",
            port=9000,
            user="test-user",
            auth_type="simple",
            namenode_web_port=9870
        )
    
    @pytest.fixture
    def mock_hdfs_client(self):
        """Mock HDFS client"""
        with patch('hdfs_cost_advisor.hdfs.client.InsecureClient') as mock_client:
            mock_instance = Mock()
            mock_client.return_value = mock_instance
            
            # Mock status method
            mock_instance.status.return_value = {
                "length": 1024 * 1024,  # 1MB
                "replication": 3,
                "blockSize": 128 * 1024 * 1024,  # 128MB
                "accessTime": 1640995200000,  # 2022-01-01
                "modificationTime": 1640995200000,
                "owner": "hadoop",
                "group": "hadoop",
                "permission": "644"
            }
            
            # Mock walk method
            mock_instance.walk.return_value = [
                ("/test", ["subdir"], ["file1.txt", "file2.txt"]),
                ("/test/subdir", [], ["file3.txt"])
            ]
            
            yield mock_instance
    
    @pytest.fixture
    def mock_requests(self):
        """Mock requests for JMX calls"""
        with patch('hdfs_cost_advisor.hdfs.client.requests') as mock_requests:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "beans": [
                    {
                        "CapacityTotal": 1000000000,
                        "CapacityUsed": 500000000,
                        "CapacityRemaining": 500000000,
                        "FilesTotal": 1000,
                        "BlocksTotal": 5000,
                        "UnderReplicatedBlocks": 0,
                        "CorruptBlocks": 0,
                        "RpcQueueTimeAvgTime": 10,
                        "RpcProcessingTimeAvgTime": 5
                    }
                ]
            }
            mock_requests.get.return_value = mock_response
            yield mock_requests
    
    def test_hdfs_client_initialization(self, hdfs_config, mock_hdfs_client):
        """Test HDFS client initialization"""
        client = HDFSClient(hdfs_config)
        
        assert client.config == hdfs_config
        assert client.jmx_base_url == "http://localhost:9870/jmx"
    
    def test_get_jmx_metrics(self, hdfs_config, mock_hdfs_client, mock_requests):
        """Test JMX metrics retrieval"""
        client = HDFSClient(hdfs_config)
        
        metrics = client.get_jmx_metrics()
        
        assert "beans" in metrics
        assert len(metrics["beans"]) > 0
        mock_requests.get.assert_called_once()
    
    def test_get_cluster_metrics(self, hdfs_config, mock_hdfs_client, mock_requests):
        """Test cluster metrics retrieval"""
        client = HDFSClient(hdfs_config)
        
        metrics = client.get_cluster_metrics()
        
        assert "filesystem" in metrics
        assert "rpc" in metrics
        assert "timestamp" in metrics
        assert metrics["filesystem"]["capacity_total"] == 1000000000
    
    def test_analyze_file_metadata(self, hdfs_config, mock_hdfs_client):
        """Test file metadata analysis"""
        client = HDFSClient(hdfs_config)
        
        metadata = client.analyze_file_metadata("/test/file.txt")
        
        assert metadata["path"] == "/test/file.txt"
        assert metadata["size"] == 1024 * 1024
        assert metadata["replication"] == 3
        assert metadata["efficiency_score"] > 0
    
    def test_scan_directory_batch(self, hdfs_config, mock_hdfs_client):
        """Test directory scanning in batches"""
        client = HDFSClient(hdfs_config)
        
        batches = list(client.scan_directory_batch("/test", 2))
        
        assert len(batches) > 0
        # Check that we get batches of file metadata
        for batch in batches:
            assert isinstance(batch, list)
            for file_info in batch:
                assert "path" in file_info
                assert "size" in file_info
    
    def test_check_path_exists(self, hdfs_config, mock_hdfs_client):
        """Test path existence check"""
        client = HDFSClient(hdfs_config)
        
        # Mock successful status call
        assert client.check_path_exists("/test/existing/path") == True
        
        # Mock failed status call
        mock_hdfs_client.status.side_effect = Exception("Path not found")
        assert client.check_path_exists("/test/nonexistent/path") == False
    
    def test_get_directory_size(self, hdfs_config, mock_hdfs_client):
        """Test directory size calculation"""
        client = HDFSClient(hdfs_config)
        
        # Mock content method
        mock_hdfs_client.content.return_value = {
            "length": 1024 * 1024 * 100,  # 100MB
            "fileCount": 10,
            "directoryCount": 2,
            "spaceConsumed": 1024 * 1024 * 300,  # 300MB (with replication)
            "quota": -1,
            "spaceQuota": -1
        }
        
        size_info = client.get_directory_size("/test")
        
        assert size_info["path"] == "/test"
        assert size_info["size"] == 1024 * 1024 * 100
        assert size_info["file_count"] == 10
        assert size_info["directory_count"] == 2

class TestHDFSMetadataAnalyzer:
    
    @pytest.fixture
    def analyzer(self):
        """Create analyzer instance"""
        return HDFSMetadataAnalyzer()
    
    @pytest.fixture
    def sample_file_metadata(self):
        """Sample file metadata for testing"""
        return [
            {
                "path": "/test/old_file.txt",
                "size": 1024 * 1024,  # 1MB
                "replication": 3,
                "access_time": 1640995200000,  # 2022-01-01 (old)
                "modification_time": 1640995200000,
                "owner": "hadoop",
                "group": "hadoop"
            },
            {
                "path": "/test/recent_file.txt",
                "size": 1024 * 1024 * 64,  # 64MB
                "replication": 3,
                "access_time": 1672531200000,  # 2023-01-01 (recent)
                "modification_time": 1672531200000,
                "owner": "hadoop",
                "group": "hadoop"
            },
            {
                "path": "/test/small_file.txt",
                "size": 1024,  # 1KB (small)
                "replication": 3,
                "access_time": 1672531200000,
                "modification_time": 1672531200000,
                "owner": "hadoop",
                "group": "hadoop"
            },
            {
                "path": "/tmp/temp_file.txt",
                "size": 1024 * 1024,
                "replication": 3,
                "access_time": 1640995200000,
                "modification_time": 1640995200000,  # Old temp file
                "owner": "hadoop",
                "group": "hadoop"
            },
            {
                "path": "/test/over_replicated.txt",
                "size": 1024 * 1024 * 10,  # 10MB
                "replication": 5,  # Over-replicated
                "access_time": 1672531200000,
                "modification_time": 1672531200000,
                "owner": "hadoop",
                "group": "hadoop"
            }
        ]
    
    def test_identify_cold_data(self, analyzer, sample_file_metadata):
        """Test cold data identification"""
        cold_data = analyzer.identify_cold_data(sample_file_metadata, 180)
        
        # Should identify files older than 180 days
        assert len(cold_data) >= 2  # old_file and temp_file
        
        # Check that cold data has required fields
        for file_info in cold_data:
            assert "classification" in file_info
            assert file_info["classification"] == "cold"
            assert "days_since_access" in file_info
            assert "cold_score" in file_info
    
    def test_detect_duplicate_candidates(self, analyzer, sample_file_metadata):
        """Test duplicate file detection"""
        duplicates = analyzer.detect_duplicate_candidates(sample_file_metadata)
        
        # Should group files by size and identify potential duplicates
        assert isinstance(duplicates, list)
        
        for file_info in duplicates:
            assert "classification" in file_info
            assert file_info["classification"] == "potential_duplicate"
            assert "group_size" in file_info
            assert "duplicate_score" in file_info
    
    def test_analyze_file_efficiency(self, analyzer, sample_file_metadata):
        """Test file efficiency analysis"""
        efficiency = analyzer.analyze_file_efficiency(sample_file_metadata)
        
        assert "total_files" in efficiency
        assert "small_files" in efficiency
        assert "small_files_count" in efficiency
        assert "small_files_percentage" in efficiency
        assert "inefficient_replication" in efficiency
        assert "over_replicated_count" in efficiency
        
        # Should identify small files
        assert efficiency["small_files_count"] >= 1
        
        # Should identify over-replicated files
        assert efficiency["over_replicated_count"] >= 1
    
    def test_identify_orphaned_temp_files(self, analyzer, sample_file_metadata):
        """Test orphaned temp file identification"""
        orphaned = analyzer.identify_orphaned_temp_files(sample_file_metadata)
        
        # Should identify temp files older than 7 days
        assert len(orphaned) >= 1
        
        for file_info in orphaned:
            assert "classification" in file_info
            assert file_info["classification"] == "orphaned_temp"
            assert "age_days" in file_info
            assert "cleanup_priority" in file_info
    
    def test_analyze_directory_structure(self, analyzer, sample_file_metadata):
        """Test directory structure analysis"""
        structure = analyzer.analyze_directory_structure(sample_file_metadata)
        
        assert "directory_stats" in structure
        assert "problematic_directories" in structure
        assert "total_directories" in structure
        assert "consolidation_candidates" in structure
        
        # Should have statistics for each directory
        assert len(structure["directory_stats"]) > 0
    
    def test_calculate_storage_waste(self, analyzer, sample_file_metadata):
        """Test storage waste calculation"""
        waste = analyzer.calculate_storage_waste(sample_file_metadata)
        
        assert "total_size_bytes" in waste
        assert "total_size_gb" in waste
        assert "replication_waste_bytes" in waste
        assert "replication_waste_gb" in waste
        assert "total_waste_bytes" in waste
        assert "waste_percentage" in waste
        
        # Should calculate waste from over-replication
        assert waste["replication_waste_bytes"] > 0
        assert waste["waste_percentage"] >= 0
    
    def test_generate_optimization_priority(self, analyzer, sample_file_metadata):
        """Test optimization priority generation"""
        optimizations = analyzer.generate_optimization_priority(sample_file_metadata)
        
        assert isinstance(optimizations, list)
        assert len(optimizations) > 0
        
        # Should have different optimization types
        optimization_types = [opt["type"] for opt in optimizations]
        assert "cold_data_migration" in optimization_types
        assert "small_file_consolidation" in optimization_types
        
        # Should be sorted by priority
        for opt in optimizations:
            assert "type" in opt
            assert "priority" in opt
            assert "impact" in opt
            assert "potential_savings_gb" in opt
            assert "description" in opt

if __name__ == "__main__":
    pytest.main([__file__])