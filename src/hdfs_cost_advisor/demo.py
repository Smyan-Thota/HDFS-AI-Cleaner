"""
Demo mode for HDFS Cost Advisor - runs without requiring actual HDFS cluster
"""

import json
import logging
import asyncio
import uuid
from typing import Dict, Any, List
from datetime import datetime
import random

class DemoHDFSClient:
    """Mock HDFS client for demo purposes"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def get_cluster_metrics(self) -> Dict[str, Any]:
        """Generate mock cluster metrics"""
        return {
            "filesystem": {
                "capacity_total": 1000 * 1024**3,  # 1TB
                "capacity_used": 600 * 1024**3,    # 600GB
                "capacity_remaining": 400 * 1024**3, # 400GB
                "files_total": 50000,
                "blocks_total": 125000,
                "under_replicated_blocks": 5,
                "corrupt_blocks": 0
            },
            "rpc": {
                "rpc_queue_time_avg": 15,
                "rpc_processing_time_avg": 8
            },
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def check_path_exists(self, path: str) -> bool:
        """Always return true for demo"""
        return True
    
    def scan_directory_batch(self, path: str, max_depth: int = 3):
        """Generate mock file metadata"""
        mock_files = []
        
        # Generate various types of files for testing
        file_types = [
            # Old files (cold data candidates)
            {
                "path": f"{path}/logs/old_log_{i}.txt",
                "size": random.randint(1024*1024, 100*1024*1024),  # 1MB - 100MB
                "replication": 3,
                "access_time": 1640995200000 - random.randint(0, 365*24*60*60*1000),  # Old
                "modification_time": 1640995200000,
                "owner": "hadoop",
                "group": "hadoop"
            } for i in range(10)
        ] + [
            # Small files
            {
                "path": f"{path}/small/file_{i}.txt",
                "size": random.randint(1024, 1024*1024),  # 1KB - 1MB
                "replication": 3,
                "access_time": 1672531200000,  # Recent
                "modification_time": 1672531200000,
                "owner": "hadoop",
                "group": "hadoop"
            } for i in range(20)
        ] + [
            # Normal files
            {
                "path": f"{path}/data/dataset_{i}.parquet",
                "size": random.randint(64*1024*1024, 512*1024*1024),  # 64MB - 512MB
                "replication": 3,
                "access_time": 1672531200000,
                "modification_time": 1672531200000,
                "owner": "hadoop",
                "group": "hadoop"
            } for i in range(15)
        ] + [
            # Temporary files
            {
                "path": f"/tmp/temp_file_{i}.tmp",
                "size": random.randint(1024*1024, 50*1024*1024),  # 1MB - 50MB
                "replication": 3,
                "access_time": 1640995200000,  # Old
                "modification_time": 1640995200000,
                "owner": "hadoop",
                "group": "hadoop"
            } for i in range(5)
        ] + [
            # Over-replicated files
            {
                "path": f"{path}/replicated/important_{i}.data",
                "size": random.randint(100*1024*1024, 1024*1024*1024),  # 100MB - 1GB
                "replication": random.randint(5, 8),  # Over-replicated
                "access_time": 1672531200000,
                "modification_time": 1672531200000,
                "owner": "hadoop",
                "group": "hadoop"
            } for i in range(8)
        ]
        
        mock_files.extend(file_types)
        
        # Return in batches
        batch_size = 25
        for i in range(0, len(mock_files), batch_size):
            yield mock_files[i:i + batch_size]

class DemoLLMClient:
    """Mock LLM client for demo purposes"""
    
    def __init__(self, provider, api_key):
        self.provider = provider
        self.api_key = api_key
        self.logger = logging.getLogger(__name__)
    
    async def analyze_hdfs_cost_optimization(self, scan_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate mock LLM analysis"""
        total_files = scan_results.get('total_files', 0)
        total_size_gb = scan_results.get('total_size_gb', 0)
        cold_data_count = len(scan_results.get('cold_data', []))
        small_files_count = len(scan_results.get('small_files', []))
        
        # Simulate LLM processing time
        await asyncio.sleep(2)
        
        return {
            "analysis_summary": f"""Based on the analysis of {total_files} files ({total_size_gb:.1f}GB total), I've identified several significant cost optimization opportunities:

1. **Cold Data Migration**: {cold_data_count} files haven't been accessed recently and could be moved to cold storage
2. **Small File Consolidation**: {small_files_count} small files are creating metadata overhead
3. **Replication Optimization**: Several files are over-replicated and consuming unnecessary space
4. **Cleanup Opportunities**: Temporary and orphaned files can be safely removed

The analysis shows potential for 35-50% cost reduction through strategic optimizations.""",
            
            "recommendations": [
                {
                    "title": "Cold Data Migration",
                    "description": f"Migrate {cold_data_count} files not accessed in 180+ days to cold storage tier",
                    "category": "cold_data",
                    "impact": "high",
                    "estimated_savings_percent": 40,
                    "estimated_savings_gb": total_size_gb * 0.3,
                    "implementation_complexity": "medium",
                    "timeline": "1-2 weeks",
                    "steps": [
                        "Identify files with access time > 180 days",
                        "Set COLD storage policy on identified files",
                        "Reduce replication factor to 1 for cold files",
                        "Monitor storage cost reduction"
                    ]
                },
                {
                    "title": "Small File Consolidation",
                    "description": f"Consolidate {small_files_count} small files to reduce metadata overhead",
                    "category": "small_files",
                    "impact": "medium",
                    "estimated_savings_percent": 15,
                    "estimated_savings_gb": small_files_count * 0.001,  # Metadata savings
                    "implementation_complexity": "high",
                    "timeline": "2-4 weeks",
                    "steps": [
                        "Identify directories with high small file counts",
                        "Create consolidation plan by file type",
                        "Implement merge jobs for small files",
                        "Update data processing pipelines"
                    ]
                },
                {
                    "title": "Cleanup Orphaned Files",
                    "description": "Remove temporary and orphaned files consuming unnecessary space",
                    "category": "cleanup",
                    "impact": "medium",
                    "estimated_savings_percent": 10,
                    "estimated_savings_gb": total_size_gb * 0.05,
                    "implementation_complexity": "low",
                    "timeline": "immediate",
                    "steps": [
                        "Identify temporary files older than 7 days",
                        "Verify files are safe to delete",
                        "Create cleanup scripts",
                        "Schedule regular cleanup jobs"
                    ]
                },
                {
                    "title": "Replication Optimization",
                    "description": "Optimize replication factors for improved cost efficiency",
                    "category": "replication",
                    "impact": "medium",
                    "estimated_savings_percent": 20,
                    "estimated_savings_gb": total_size_gb * 0.15,
                    "implementation_complexity": "low",
                    "timeline": "1 week",
                    "steps": [
                        "Identify over-replicated files",
                        "Assess criticality of each file",
                        "Reduce replication to optimal levels",
                        "Monitor cluster balance"
                    ]
                }
            ],
            
            "cost_calculations": {
                "current_monthly_cost": total_size_gb * 0.04 * 3,  # $0.04/GB with 3x replication
                "optimized_monthly_cost": total_size_gb * 0.04 * 3 * 0.6,  # 40% reduction
                "monthly_savings": total_size_gb * 0.04 * 3 * 0.4,
                "annual_savings": total_size_gb * 0.04 * 3 * 0.4 * 12
            },
            
            "risk_assessment": {
                "data_loss_risk": "low",
                "performance_impact": "positive",
                "downtime_required": "minimal"
            },
            
            "monitoring_recommendations": [
                "Track storage utilization trends",
                "Monitor file access patterns",
                "Set up cost tracking dashboards",
                "Implement automated small file detection"
            ],
            
            "confidence_score": 0.92
        }

class DemoMCPServer:
    """Demo MCP server that works without real HDFS cluster"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        
        # Initialize with demo clients
        from .utils.config import StorageCosts
        from .cost.calculator import CostCalculator
        
        # Use demo clients
        self.hdfs_client = DemoHDFSClient(None)
        self.llm_client = DemoLLMClient("demo", "demo-key")
        self.cost_calculator = CostCalculator(StorageCosts())
        
        self.logger.info("Demo MCP Server initialized")

    async def scan_hdfs(self, paths: List[str], scan_depth: int = 3) -> Dict[str, Any]:
        """Demo HDFS scan"""
        try:
            from .endpoints import scan
            from .hdfs.analyzer import HDFSMetadataAnalyzer
            
            scan_id = str(uuid.uuid4())
            self.logger.info(f"Demo scan {scan_id} for paths: {paths}")
            
            # Get mock cluster metrics
            cluster_metrics = self.hdfs_client.get_cluster_metrics()
            
            # Get mock file data
            all_files = []
            for path in paths:
                for batch in self.hdfs_client.scan_directory_batch(path, scan_depth):
                    all_files.extend(batch)
            
            # Analyze with real analyzer
            analyzer = HDFSMetadataAnalyzer()
            cold_data = analyzer.identify_cold_data(all_files)
            duplicates = analyzer.detect_duplicate_candidates(all_files)
            efficiency = analyzer.analyze_file_efficiency(all_files)
            orphaned = analyzer.identify_orphaned_temp_files(all_files)
            
            total_size = sum(file.get("size", 0) for file in all_files)
            total_size_gb = total_size / (1024 ** 3)
            
            result = {
                "scan_id": scan_id,
                "status": "completed",
                "message": f"Demo scan completed - {len(all_files)} files analyzed",
                "total_files": len(all_files),
                "total_size_gb": total_size_gb,
                "cold_data": cold_data,
                "duplicate_candidates": duplicates,
                "small_files": efficiency.get("small_files", []),
                "orphaned_files": orphaned,
                "over_replicated_files": efficiency.get("inefficient_replication", []),
                "efficiency_analysis": efficiency,
                "cluster_metrics": cluster_metrics,
                "demo_mode": True
            }
            
            # Store in scan module for later retrieval
            scan.scan_results_storage[scan_id] = result
            
            return result
            
        except Exception as e:
            self.logger.error(f"Demo scan failed: {e}")
            return {
                "error": str(e),
                "status": "failed",
                "demo_mode": True
            }

    async def optimize_costs(self, scan_id: str) -> Dict[str, Any]:
        """Demo cost optimization"""
        try:
            from .endpoints import scan
            from .endpoints.optimize import optimization_results_storage
            from .endpoints.generate_script import store_optimization_plan
            
            scan_results = scan.get_scan_results(scan_id)
            
            # Get LLM analysis
            llm_analysis = await self.llm_client.analyze_hdfs_cost_optimization(scan_results)
            
            # Create optimization plan
            optimization_id = str(uuid.uuid4())
            optimization_plan = {
                "plan_id": optimization_id,
                "optimizations": llm_analysis["recommendations"],
                "total_monthly_savings": llm_analysis["cost_calculations"]["monthly_savings"],
                "total_annual_savings": llm_analysis["cost_calculations"]["annual_savings"],
                "affected_data_gb": scan_results["total_size_gb"] * 0.6,
                "demo_mode": True
            }
            
            result = {
                "optimization_id": optimization_id,
                "scan_id": scan_id,
                "status": "completed",
                "llm_analysis": llm_analysis,
                "optimization_plan": optimization_plan,
                "summary": {
                    "total_monthly_savings": llm_analysis["cost_calculations"]["monthly_savings"],
                    "total_annual_savings": llm_analysis["cost_calculations"]["annual_savings"],
                    "affected_data_gb": scan_results["total_size_gb"] * 0.6
                },
                "demo_mode": True
            }
            
            # Store results
            optimization_results_storage[optimization_id] = result
            store_optimization_plan(optimization_id, optimization_plan)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Demo optimization failed: {e}")
            return {
                "error": str(e),
                "status": "failed",
                "demo_mode": True
            }

    async def generate_script(self, optimization_id: str) -> str:
        """Demo script generation"""
        try:
            from .endpoints import generate_script
            script = generate_script.create_optimization_script(optimization_id)
            return f"# DEMO MODE SCRIPT\n# This is a demonstration script\n\n{script}"
            
        except Exception as e:
            return f"# Demo script generation failed: {str(e)}"

    async def get_summary(self, scan_id: str) -> Dict[str, Any]:
        """Demo summary generation"""
        try:
            from .endpoints import summary
            result = summary.generate_summary(scan_id, self.cost_calculator)
            result["demo_mode"] = True
            return result
            
        except Exception as e:
            return {
                "error": str(e),
                "status": "failed",
                "demo_mode": True
            }

    async def get_cluster_health(self) -> Dict[str, Any]:
        """Demo cluster health"""
        try:
            metrics = self.hdfs_client.get_cluster_metrics()
            return {
                "cluster_metrics": metrics,
                "status": "healthy",
                "timestamp": metrics.get("timestamp"),
                "demo_mode": True
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "status": "failed",
                "demo_mode": True
            }

# Demo CLI
async def demo_cli():
    """Demo command-line interface"""
    print("=== HDFS Cost Advisor - DEMO MODE ===")
    print("This demo simulates HDFS cost analysis without requiring a real cluster")
    print()
    print("Available commands:")
    print("1. scan [paths...] - Scan HDFS paths (demo data)")
    print("2. optimize [scan_id] - Generate optimization recommendations")
    print("3. script [opt_id] - Generate optimization script")
    print("4. summary [scan_id] - Get analysis summary")
    print("5. health - Get cluster health (demo)")
    print("6. quit - Exit")
    print()
    
    server = DemoMCPServer()
    
    while True:
        try:
            command = input("demo> ").strip().split()
            if not command:
                continue
                
            if command[0] == "quit":
                break
            elif command[0] == "scan":
                paths = command[1:] if len(command) > 1 else ["/data", "/logs"]
                print(f"Scanning paths: {paths}")
                result = await server.scan_hdfs(paths)
                print(json.dumps({k: v for k, v in result.items() if k not in ['cold_data', 'small_files', 'orphaned_files']}, indent=2))
                print(f"\nScan ID: {result.get('scan_id')}")
            elif command[0] == "optimize" and len(command) > 1:
                print(f"Optimizing scan: {command[1]}")
                result = await server.optimize_costs(command[1])
                if 'llm_analysis' in result:
                    print(f"Analysis: {result['llm_analysis']['analysis_summary']}")
                    print(f"Recommendations: {len(result['llm_analysis']['recommendations'])}")
                    print(f"Monthly savings: ${result['summary']['total_monthly_savings']:.2f}")
                    print(f"Optimization ID: {result.get('optimization_id')}")
                else:
                    print(json.dumps(result, indent=2))
            elif command[0] == "script" and len(command) > 1:
                print(f"Generating script for: {command[1]}")
                result = await server.generate_script(command[1])
                print(result[:500] + "..." if len(result) > 500 else result)
            elif command[0] == "summary" and len(command) > 1:
                print(f"Getting summary for: {command[1]}")
                result = await server.get_summary(command[1])
                if 'optimization_opportunities' in result:
                    print(f"Total files: {result['scan_info']['total_files']}")
                    print(f"Total size: {result['scan_info']['total_size_gb']:.1f} GB")
                    print(f"Projected savings: ${result['projected_savings']['projected_monthly_savings']:.2f}/month")
                else:
                    print(json.dumps(result, indent=2))
            elif command[0] == "health":
                result = await server.get_cluster_health()
                print(f"Cluster status: {result['status']}")
                if 'cluster_metrics' in result:
                    fs = result['cluster_metrics']['filesystem']
                    print(f"Capacity: {fs['capacity_used']/fs['capacity_total']*100:.1f}% used")
                    print(f"Files: {fs['files_total']:,}")
            else:
                print("Invalid command. Available: scan, optimize, script, summary, health, quit")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(demo_cli())