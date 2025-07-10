import logging
from typing import Dict, Any, List
from datetime import datetime
import uuid
from ..hdfs.client import HDFSClient
from ..hdfs.analyzer import HDFSMetadataAnalyzer

logger = logging.getLogger(__name__)

# In-memory storage for scan results (replace with database in production)
scan_results_storage = {}

def execute_scan(hdfs_client: HDFSClient, paths: List[str], depth: int) -> Dict[str, Any]:
    """Execute comprehensive HDFS scan"""
    scan_id = str(uuid.uuid4())
    
    try:
        logger.info(f"Starting scan {scan_id} for paths: {paths}")
        
        # Get cluster metrics
        cluster_metrics = hdfs_client.get_cluster_metrics()
        
        # Scan file metadata
        all_files = []
        for path in paths:
            logger.info(f"Scanning path: {path}")
            
            # Check if path exists
            if not hdfs_client.check_path_exists(path):
                logger.warning(f"Path does not exist: {path}")
                continue
            
            # Scan directory in batches
            file_count = 0
            for batch in hdfs_client.scan_directory_batch(path, depth):
                all_files.extend(batch)
                file_count += len(batch)
                logger.info(f"Processed {file_count} files from {path}")
        
        if not all_files:
            logger.warning("No files found in specified paths")
            return {
                "scan_id": scan_id,
                "status": "completed",
                "message": "No files found in specified paths",
                "total_files": 0,
                "total_size_gb": 0,
                "cold_data": [],
                "duplicate_candidates": [],
                "small_files": [],
                "orphaned_files": [],
                "cluster_metrics": cluster_metrics,
                "scan_started": datetime.utcnow().isoformat(),
                "scan_completed": datetime.utcnow().isoformat()
            }
        
        logger.info(f"Analyzing {len(all_files)} files")
        
        # Analyze metadata
        analyzer = HDFSMetadataAnalyzer()
        
        # Run various analyses
        cold_data = analyzer.identify_cold_data(all_files)
        duplicates = analyzer.detect_duplicate_candidates(all_files)
        efficiency = analyzer.analyze_file_efficiency(all_files)
        orphaned = analyzer.identify_orphaned_temp_files(all_files)
        directory_analysis = analyzer.analyze_directory_structure(all_files)
        waste_analysis = analyzer.calculate_storage_waste(all_files)
        optimization_priorities = analyzer.generate_optimization_priority(all_files)
        
        # Calculate totals
        total_size = sum(file.get("size", 0) for file in all_files)
        total_size_gb = total_size / (1024 ** 3)
        
        # Create comprehensive results
        scan_results = {
            "scan_id": scan_id,
            "status": "completed",
            "message": f"Successfully scanned {len(all_files)} files",
            "scan_started": datetime.utcnow().isoformat(),
            "scan_completed": datetime.utcnow().isoformat(),
            "scanned_paths": paths,
            "scan_depth": depth,
            
            # Basic metrics
            "total_files": len(all_files),
            "total_size_bytes": total_size,
            "total_size_gb": total_size_gb,
            
            # Analysis results
            "cold_data": cold_data,
            "duplicate_candidates": duplicates,
            "small_files": efficiency.get("small_files", []),
            "empty_files": efficiency.get("empty_files", []),
            "orphaned_files": orphaned,
            "over_replicated_files": efficiency.get("inefficient_replication", []),
            
            # Efficiency analysis
            "efficiency_analysis": {
                "small_files_count": efficiency.get("small_files_count", 0),
                "small_files_percentage": efficiency.get("small_files_percentage", 0),
                "empty_files_count": efficiency.get("empty_files_count", 0),
                "over_replicated_count": efficiency.get("over_replicated_count", 0),
                "over_replicated_percentage": efficiency.get("over_replicated_percentage", 0),
                "efficiency_summary": efficiency.get("efficiency_summary", {})
            },
            
            # Directory analysis
            "directory_analysis": directory_analysis,
            
            # Waste analysis
            "waste_analysis": waste_analysis,
            
            # Optimization priorities
            "optimization_priorities": optimization_priorities,
            
            # Cluster metrics
            "cluster_metrics": cluster_metrics
        }
        
        # Store results for later retrieval
        scan_results_storage[scan_id] = scan_results
        
        logger.info(f"Scan {scan_id} completed successfully")
        return scan_results
        
    except Exception as e:
        logger.error(f"Scan {scan_id} failed: {e}")
        error_result = {
            "scan_id": scan_id,
            "status": "failed",
            "message": f"Scan failed: {str(e)}",
            "error": str(e),
            "scan_started": datetime.utcnow().isoformat(),
            "scan_completed": datetime.utcnow().isoformat(),
            "scanned_paths": paths,
            "scan_depth": depth,
            "total_files": 0,
            "total_size_gb": 0,
            "cold_data": [],
            "duplicate_candidates": [],
            "small_files": [],
            "orphaned_files": [],
            "cluster_metrics": {}
        }
        
        scan_results_storage[scan_id] = error_result
        raise

def get_scan_results(scan_id: str) -> Dict[str, Any]:
    """Retrieve scan results by ID"""
    if scan_id not in scan_results_storage:
        raise ValueError(f"Scan results not found for ID: {scan_id}")
    
    return scan_results_storage[scan_id]

def list_scans() -> List[Dict[str, Any]]:
    """List all available scans"""
    return [
        {
            "scan_id": scan_id,
            "status": results.get("status", "unknown"),
            "scan_started": results.get("scan_started"),
            "scan_completed": results.get("scan_completed"),
            "total_files": results.get("total_files", 0),
            "total_size_gb": results.get("total_size_gb", 0),
            "scanned_paths": results.get("scanned_paths", [])
        }
        for scan_id, results in scan_results_storage.items()
    ]

def get_scan_summary(scan_id: str) -> Dict[str, Any]:
    """Get a summary of scan results"""
    results = get_scan_results(scan_id)
    
    return {
        "scan_id": scan_id,
        "status": results.get("status"),
        "total_files": results.get("total_files", 0),
        "total_size_gb": results.get("total_size_gb", 0),
        "optimization_opportunities": {
            "cold_data_files": len(results.get("cold_data", [])),
            "small_files": len(results.get("small_files", [])),
            "empty_files": len(results.get("empty_files", [])),
            "orphaned_files": len(results.get("orphaned_files", [])),
            "over_replicated_files": len(results.get("over_replicated_files", [])),
            "duplicate_candidates": len(results.get("duplicate_candidates", []))
        },
        "potential_savings": {
            "waste_percentage": results.get("waste_analysis", {}).get("waste_percentage", 0),
            "waste_gb": results.get("waste_analysis", {}).get("total_waste_bytes", 0) / (1024 ** 3)
        },
        "cluster_health": {
            "capacity_used_gb": results.get("cluster_metrics", {}).get("filesystem", {}).get("capacity_used", 0) / (1024 ** 3),
            "capacity_total_gb": results.get("cluster_metrics", {}).get("filesystem", {}).get("capacity_total", 0) / (1024 ** 3),
            "under_replicated_blocks": results.get("cluster_metrics", {}).get("filesystem", {}).get("under_replicated_blocks", 0),
            "corrupt_blocks": results.get("cluster_metrics", {}).get("filesystem", {}).get("corrupt_blocks", 0)
        }
    }

def delete_scan_results(scan_id: str) -> bool:
    """Delete scan results"""
    if scan_id in scan_results_storage:
        del scan_results_storage[scan_id]
        logger.info(f"Deleted scan results for {scan_id}")
        return True
    return False