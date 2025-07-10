import pytest
import os
import sys

# Add src to Python path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

@pytest.fixture(scope="session")
def test_config():
    """Test configuration fixture"""
    return {
        "hdfs": {
            "host": "localhost",
            "port": 9000,
            "user": "test-user",
            "auth_type": "simple",
            "namenode_web_port": 9870
        },
        "llm": {
            "provider": "anthropic",
            "api_key": "test-api-key",
            "model_name": "claude-3-sonnet-20240229",
            "max_tokens": 3000,
            "temperature": 0.3
        },
        "cost": {
            "standard_storage_cost_per_gb": 0.04,
            "cold_storage_cost_per_gb": 0.01,
            "archive_storage_cost_per_gb": 0.005,
            "metadata_cost_per_file": 0.0001,
            "network_cost_per_gb": 0.01
        }
    }

@pytest.fixture
def sample_scan_results():
    """Sample scan results for testing"""
    return {
        "scan_id": "test-scan-123",
        "status": "completed",
        "total_files": 1000,
        "total_size_gb": 100.0,
        "cold_data": [
            {
                "path": "/test/old_file.txt",
                "size": 1024 * 1024,  # 1MB
                "days_since_access": 365,
                "classification": "cold"
            }
        ],
        "small_files": [
            {
                "path": "/test/small_file.txt",
                "size": 1024,  # 1KB
                "classification": "small_file"
            }
        ],
        "orphaned_files": [
            {
                "path": "/tmp/orphaned.txt",
                "size": 1024 * 1024,  # 1MB
                "age_days": 30,
                "classification": "orphaned_temp"
            }
        ],
        "over_replicated_files": [
            {
                "path": "/test/over_replicated.txt",
                "size": 10 * 1024 * 1024,  # 10MB
                "current_replication": 5,
                "suggested_replication": 3,
                "classification": "over_replicated"
            }
        ],
        "efficiency_analysis": {
            "small_files_count": 100,
            "small_files_percentage": 10.0,
            "over_replicated_count": 50,
            "over_replicated_percentage": 5.0
        },
        "waste_analysis": {
            "total_size_bytes": 100 * 1024 * 1024 * 1024,  # 100GB
            "total_size_gb": 100.0,
            "replication_waste_bytes": 20 * 1024 * 1024 * 1024,  # 20GB
            "replication_waste_gb": 20.0,
            "waste_percentage": 20.0
        },
        "cluster_metrics": {
            "filesystem": {
                "capacity_total": 1000 * 1024 * 1024 * 1024,  # 1TB
                "capacity_used": 500 * 1024 * 1024 * 1024,  # 500GB
                "capacity_remaining": 500 * 1024 * 1024 * 1024,  # 500GB
                "files_total": 1000,
                "blocks_total": 5000,
                "under_replicated_blocks": 0,
                "corrupt_blocks": 0
            },
            "rpc": {
                "rpc_queue_time_avg": 10,
                "rpc_processing_time_avg": 5
            },
            "timestamp": "2023-01-01T00:00:00.000Z"
        }
    }

@pytest.fixture
def sample_optimization_results():
    """Sample optimization results for testing"""
    return {
        "optimization_id": "test-opt-123",
        "scan_id": "test-scan-123",
        "status": "completed",
        "llm_analysis": {
            "analysis_summary": "Test analysis summary",
            "recommendations": [
                {
                    "title": "Cold Data Migration",
                    "description": "Move old files to cold storage",
                    "category": "cold_data",
                    "impact": "high",
                    "estimated_savings_percent": 30,
                    "estimated_savings_gb": 30,
                    "implementation_complexity": "medium",
                    "timeline": "1-2 weeks"
                }
            ],
            "cost_calculations": {
                "current_monthly_cost": 400,
                "optimized_monthly_cost": 280,
                "monthly_savings": 120,
                "annual_savings": 1440
            }
        },
        "summary": {
            "total_monthly_savings": 120,
            "total_annual_savings": 1440,
            "affected_data_gb": 30,
            "optimization_categories": ["cold_data"]
        }
    }

@pytest.fixture
def mock_environment():
    """Mock environment variables for testing"""
    env_vars = {
        "HDFS_HOST": "test-namenode",
        "HDFS_PORT": "9000",
        "HDFS_USER": "test-user",
        "LLM_PROVIDER": "anthropic",
        "LLM_API_KEY": "test-api-key",
        "LOG_LEVEL": "DEBUG"
    }
    
    # Set environment variables
    for key, value in env_vars.items():
        os.environ[key] = value
    
    yield env_vars
    
    # Clean up environment variables
    for key in env_vars:
        if key in os.environ:
            del os.environ[key]

@pytest.fixture
def temp_directory(tmp_path):
    """Create a temporary directory for tests"""
    test_dir = tmp_path / "hdfs_test"
    test_dir.mkdir()
    
    # Create some test files
    (test_dir / "test_file.txt").write_text("test content")
    (test_dir / "logs").mkdir()
    (test_dir / "data").mkdir()
    
    return test_dir