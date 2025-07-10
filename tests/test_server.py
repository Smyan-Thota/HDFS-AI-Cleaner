import pytest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from hdfs_cost_advisor.server import mcp, scan_hdfs, optimize_costs, generate_script, get_summary

class TestHDFSCostAdvisorServer:
    
    @pytest.fixture
    def mock_hdfs_client(self):
        """Mock HDFS client for testing"""
        mock_client = Mock()
        mock_client.get_cluster_metrics.return_value = {
            "filesystem": {
                "capacity_total": 1000000000,
                "capacity_used": 500000000,
                "capacity_remaining": 500000000,
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
        
        mock_client.check_path_exists.return_value = True
        mock_client.scan_directory_batch.return_value = iter([
            [
                {
                    "path": "/test/file1.txt",
                    "size": 1024 * 1024,  # 1MB
                    "replication": 3,
                    "access_time": 1640995200000,  # 2022-01-01
                    "modification_time": 1640995200000,
                    "owner": "hadoop",
                    "group": "hadoop"
                },
                {
                    "path": "/test/file2.txt",
                    "size": 10 * 1024 * 1024,  # 10MB
                    "replication": 3,
                    "access_time": 1672531200000,  # 2023-01-01
                    "modification_time": 1672531200000,
                    "owner": "hadoop",
                    "group": "hadoop"
                }
            ]
        ])
        
        return mock_client
    
    @pytest.fixture
    def mock_llm_client(self):
        """Mock LLM client for testing"""
        mock_client = Mock()
        mock_client.analyze_hdfs_cost_optimization = AsyncMock(return_value={
            "analysis_summary": "Test analysis summary",
            "recommendations": [
                {
                    "title": "Cold Data Migration",
                    "description": "Move old files to cold storage",
                    "category": "cold_data",
                    "impact": "high",
                    "estimated_savings_percent": 30,
                    "estimated_savings_gb": 100,
                    "implementation_complexity": "medium",
                    "timeline": "1-2 weeks",
                    "steps": ["Identify cold files", "Set cold policy", "Monitor"]
                }
            ],
            "cost_calculations": {
                "current_monthly_cost": 1000,
                "optimized_monthly_cost": 700,
                "monthly_savings": 300,
                "annual_savings": 3600
            },
            "risk_assessment": {
                "data_loss_risk": "low",
                "performance_impact": "positive",
                "downtime_required": "minimal"
            },
            "monitoring_recommendations": ["Storage utilization", "Access patterns"],
            "confidence_score": 0.85
        })
        
        return mock_client
    
    @pytest.fixture
    def mock_cost_calculator(self):
        """Mock cost calculator for testing"""
        mock_calculator = Mock()
        mock_calculator.calculate_current_costs.return_value = {
            "storage_cost": 800,
            "metadata_cost": 50,
            "small_file_overhead": 100,
            "network_cost": 50,
            "total_monthly_cost": 1000,
            "total_annual_cost": 12000,
            "cost_per_gb": 0.12
        }
        
        return mock_calculator
    
    def test_scan_hdfs_success(self, mock_hdfs_client):
        """Test successful HDFS scan"""
        with patch('hdfs_cost_advisor.server.hdfs_client', mock_hdfs_client):
            result = scan_hdfs(["/test/data"], 3)
            
            assert result["status"] == "completed"
            assert result["total_files"] == 2
            assert result["total_size_gb"] > 0
            assert len(result["cold_data"]) >= 0
            assert len(result["small_files"]) >= 0
            assert "scan_id" in result
    
    def test_scan_hdfs_empty_paths(self, mock_hdfs_client):
        """Test scan with empty paths"""
        with pytest.raises(Exception):
            scan_hdfs([], 3)
    
    def test_scan_hdfs_invalid_depth(self, mock_hdfs_client):
        """Test scan with invalid depth"""
        with patch('hdfs_cost_advisor.server.hdfs_client', mock_hdfs_client):
            result = scan_hdfs(["/test/data"], 0)
            # Should still work with minimum depth
            assert "scan_id" in result
    
    @pytest.mark.asyncio
    async def test_optimize_costs_success(self, mock_hdfs_client, mock_llm_client, mock_cost_calculator):
        """Test successful cost optimization"""
        # First create a scan
        with patch('hdfs_cost_advisor.server.hdfs_client', mock_hdfs_client):
            scan_result = scan_hdfs(["/test/data"], 3)
            scan_id = scan_result["scan_id"]
        
        # Then optimize
        with patch('hdfs_cost_advisor.server.hdfs_client', mock_hdfs_client), \
             patch('hdfs_cost_advisor.server.llm_client', mock_llm_client), \
             patch('hdfs_cost_advisor.server.cost_calculator', mock_cost_calculator):
            
            result = optimize_costs(scan_id)
            
            assert result["status"] == "completed"
            assert result["scan_id"] == scan_id
            assert "llm_analysis" in result
            assert "optimization_plan" in result
            assert "summary" in result
    
    def test_optimize_costs_invalid_scan_id(self, mock_hdfs_client, mock_llm_client, mock_cost_calculator):
        """Test optimization with invalid scan ID"""
        with patch('hdfs_cost_advisor.server.hdfs_client', mock_hdfs_client), \
             patch('hdfs_cost_advisor.server.llm_client', mock_llm_client), \
             patch('hdfs_cost_advisor.server.cost_calculator', mock_cost_calculator):
            
            result = optimize_costs("invalid-scan-id")
            assert result["status"] == "failed"
            assert "error" in result
    
    def test_generate_script_success(self):
        """Test successful script generation"""
        # Mock the optimization plan storage
        with patch('hdfs_cost_advisor.endpoints.generate_script.script_generator') as mock_generator:
            mock_generator.get_optimization_plan.return_value = {
                "plan_id": "test-plan",
                "optimizations": [
                    {
                        "category": "cold_data",
                        "files": [
                            {
                                "path": "/test/cold.txt",
                                "size": 1024,
                                "size_gb": 0.001
                            }
                        ]
                    }
                ],
                "total_monthly_savings": 100,
                "total_annual_savings": 1200,
                "affected_data_gb": 50
            }
            
            result = generate_script("test-optimization-id")
            
            assert isinstance(result, str)
            assert "#!/bin/bash" in result
            assert "HDFS Cost Optimization Script" in result
    
    def test_generate_script_invalid_id(self):
        """Test script generation with invalid optimization ID"""
        result = generate_script("invalid-optimization-id")
        
        assert isinstance(result, str)
        assert "Script generation failed" in result
    
    def test_get_summary_success(self, mock_hdfs_client, mock_cost_calculator):
        """Test successful summary generation"""
        # First create a scan
        with patch('hdfs_cost_advisor.server.hdfs_client', mock_hdfs_client):
            scan_result = scan_hdfs(["/test/data"], 3)
            scan_id = scan_result["scan_id"]
        
        # Then get summary
        with patch('hdfs_cost_advisor.server.cost_calculator', mock_cost_calculator):
            result = get_summary(scan_id)
            
            assert result["status"] == "completed"
            assert result["scan_id"] == scan_id
            assert "scan_info" in result
            assert "current_costs" in result
            assert "optimization_opportunities" in result
    
    def test_get_summary_invalid_scan_id(self, mock_cost_calculator):
        """Test summary generation with invalid scan ID"""
        with patch('hdfs_cost_advisor.server.cost_calculator', mock_cost_calculator):
            result = get_summary("invalid-scan-id")
            
            assert result["status"] == "failed"
            assert "error" in result

if __name__ == "__main__":
    pytest.main([__file__])