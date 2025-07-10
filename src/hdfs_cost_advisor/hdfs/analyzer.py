from typing import Dict, List, Any
import logging
from collections import defaultdict
from datetime import datetime, timedelta

class HDFSMetadataAnalyzer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def identify_cold_data(self, file_metadata: List[Dict[str, Any]], 
                          cold_threshold_days: int = 180) -> List[Dict[str, Any]]:
        """Identify cold data based on access patterns"""
        cold_data = []
        current_time = datetime.now().timestamp() * 1000  # Convert to milliseconds
        cold_threshold = current_time - (cold_threshold_days * 24 * 60 * 60 * 1000)
        
        for file_info in file_metadata:
            access_time = file_info.get("access_time", 0)
            if access_time < cold_threshold:
                days_since_access = (current_time - access_time) / (24 * 60 * 60 * 1000)
                cold_data.append({
                    **file_info,
                    "classification": "cold",
                    "days_since_access": days_since_access,
                    "cold_score": min(days_since_access / cold_threshold_days, 1.0)
                })
        
        # Sort by coldness score (highest first)
        cold_data.sort(key=lambda x: x["cold_score"], reverse=True)
        return cold_data
    
    def detect_duplicate_candidates(self, file_metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Detect potential duplicate files based on size and name patterns"""
        size_groups = defaultdict(list)
        
        # Group files by size
        for file_info in file_metadata:
            size = file_info.get("size", 0)
            if size > 0:
                size_groups[size].append(file_info)
        
        # Identify potential duplicates
        duplicate_candidates = []
        for size, files in size_groups.items():
            if len(files) > 1:
                # Further analyze files with same size
                for file_info in files:
                    path = file_info.get("path", "")
                    filename = path.split("/")[-1] if path else ""
                    
                    duplicate_candidates.append({
                        **file_info,
                        "classification": "potential_duplicate",
                        "group_size": len(files),
                        "filename": filename,
                        "duplicate_score": len(files) / 10.0  # Normalize score
                    })
        
        # Sort by duplicate score (highest first)
        duplicate_candidates.sort(key=lambda x: x["duplicate_score"], reverse=True)
        return duplicate_candidates
    
    def analyze_file_efficiency(self, file_metadata: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze file layout efficiency and small file problems"""
        total_files = len(file_metadata)
        small_files = []
        inefficient_replication = []
        empty_files = []
        
        for file_info in file_metadata:
            size = file_info.get("size", 0)
            replication = file_info.get("replication", 1)
            
            # Identify empty files
            if size == 0:
                empty_files.append({
                    **file_info,
                    "classification": "empty_file",
                    "efficiency_impact": "medium"
                })
            
            # Identify small files (< 64MB)
            elif size < 64 * 1024 * 1024:
                small_files.append({
                    **file_info,
                    "classification": "small_file",
                    "efficiency_impact": "high" if size < 1024 * 1024 else "medium",
                    "size_mb": size / (1024 * 1024)
                })
            
            # Identify over-replicated files
            if replication > 3:
                inefficient_replication.append({
                    **file_info,
                    "classification": "over_replicated",
                    "current_replication": replication,
                    "suggested_replication": 3,
                    "excess_replicas": replication - 3
                })
        
        # Calculate efficiency metrics
        small_files_percentage = (len(small_files) / total_files) * 100 if total_files > 0 else 0
        over_replicated_percentage = (len(inefficient_replication) / total_files) * 100 if total_files > 0 else 0
        
        return {
            "total_files": total_files,
            "small_files": small_files,
            "small_files_count": len(small_files),
            "small_files_percentage": small_files_percentage,
            "empty_files": empty_files,
            "empty_files_count": len(empty_files),
            "inefficient_replication": inefficient_replication,
            "over_replicated_count": len(inefficient_replication),
            "over_replicated_percentage": over_replicated_percentage,
            "efficiency_summary": {
                "critical_issues": len(empty_files) + len([f for f in small_files if f["efficiency_impact"] == "high"]),
                "moderate_issues": len([f for f in small_files if f["efficiency_impact"] == "medium"]),
                "storage_waste_factor": (len(small_files) * 0.1) + (len(inefficient_replication) * 0.2)
            }
        }
    
    def identify_orphaned_temp_files(self, file_metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Identify orphaned temporary files"""
        orphaned_files = []
        temp_patterns = [
            "/tmp/", "/var/tmp/", "/_temporary/", "/temp/",
            ".tmp", ".temp", ".bak", ".backup", "_tmp", "_temp"
        ]
        
        current_time = datetime.now().timestamp() * 1000
        
        for file_info in file_metadata:
            path = file_info.get("path", "")
            modification_time = file_info.get("modification_time", 0)
            
            # Check for temporary file patterns
            is_temp = any(pattern in path.lower() for pattern in temp_patterns)
            
            if is_temp:
                file_age_days = (current_time - modification_time) / (24 * 60 * 60 * 1000)
                
                # Consider as orphaned if older than 7 days
                if file_age_days > 7:
                    cleanup_priority = "high" if file_age_days > 30 else "medium"
                    if file_age_days > 90:
                        cleanup_priority = "critical"
                    
                    orphaned_files.append({
                        **file_info,
                        "classification": "orphaned_temp",
                        "age_days": file_age_days,
                        "cleanup_priority": cleanup_priority,
                        "temp_pattern": next((p for p in temp_patterns if p in path.lower()), "unknown")
                    })
        
        # Sort by age (oldest first)
        orphaned_files.sort(key=lambda x: x["age_days"], reverse=True)
        return orphaned_files
    
    def analyze_directory_structure(self, file_metadata: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze directory structure for optimization opportunities"""
        directory_stats = defaultdict(lambda: {
            "file_count": 0,
            "total_size": 0,
            "small_files": 0,
            "large_files": 0,
            "avg_file_size": 0
        })
        
        for file_info in file_metadata:
            path = file_info.get("path", "")
            size = file_info.get("size", 0)
            
            # Extract directory path
            directory = "/".join(path.split("/")[:-1]) if "/" in path else "/"
            
            # Update directory statistics
            directory_stats[directory]["file_count"] += 1
            directory_stats[directory]["total_size"] += size
            
            if size < 64 * 1024 * 1024:  # Less than 64MB
                directory_stats[directory]["small_files"] += 1
            else:
                directory_stats[directory]["large_files"] += 1
        
        # Calculate averages and identify problematic directories
        problematic_directories = []
        for directory, stats in directory_stats.items():
            if stats["file_count"] > 0:
                stats["avg_file_size"] = stats["total_size"] / stats["file_count"]
                stats["small_file_ratio"] = stats["small_files"] / stats["file_count"]
                
                # Identify directories with high small file ratios
                if stats["small_file_ratio"] > 0.7 and stats["file_count"] > 10:
                    problematic_directories.append({
                        "directory": directory,
                        "issue": "high_small_file_ratio",
                        "small_file_ratio": stats["small_file_ratio"],
                        "file_count": stats["file_count"],
                        "total_size_mb": stats["total_size"] / (1024 * 1024),
                        "optimization_potential": "file_consolidation"
                    })
        
        return {
            "directory_stats": dict(directory_stats),
            "problematic_directories": problematic_directories,
            "total_directories": len(directory_stats),
            "consolidation_candidates": len(problematic_directories)
        }
    
    def calculate_storage_waste(self, file_metadata: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate various forms of storage waste"""
        total_size = sum(file_info.get("size", 0) for file_info in file_metadata)
        
        # Calculate waste from over-replication
        replication_waste = 0
        for file_info in file_metadata:
            size = file_info.get("size", 0)
            replication = file_info.get("replication", 1)
            if replication > 3:
                replication_waste += size * (replication - 3)
        
        # Calculate waste from empty files
        empty_file_waste = sum(
            file_info.get("block_size", 0) for file_info in file_metadata
            if file_info.get("size", 0) == 0
        )
        
        # Calculate waste from small files (metadata overhead)
        small_file_overhead = len([
            f for f in file_metadata if f.get("size", 0) < 64 * 1024 * 1024
        ]) * 150  # Assume 150 bytes metadata overhead per small file
        
        return {
            "total_size_bytes": total_size,
            "total_size_gb": total_size / (1024 ** 3),
            "replication_waste_bytes": replication_waste,
            "replication_waste_gb": replication_waste / (1024 ** 3),
            "empty_file_waste_bytes": empty_file_waste,
            "small_file_overhead_bytes": small_file_overhead,
            "total_waste_bytes": replication_waste + empty_file_waste + small_file_overhead,
            "waste_percentage": ((replication_waste + empty_file_waste + small_file_overhead) / total_size) * 100 if total_size > 0 else 0
        }
    
    def generate_optimization_priority(self, file_metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate prioritized list of optimization opportunities"""
        optimizations = []
        
        # Analyze different aspects
        cold_data = self.identify_cold_data(file_metadata)
        efficiency_analysis = self.analyze_file_efficiency(file_metadata)
        orphaned_files = self.identify_orphaned_temp_files(file_metadata)
        duplicates = self.detect_duplicate_candidates(file_metadata)
        waste_analysis = self.calculate_storage_waste(file_metadata)
        
        # Add optimization opportunities based on analysis
        if cold_data:
            optimizations.append({
                "type": "cold_data_migration",
                "priority": "high",
                "impact": "high",
                "affected_files": len(cold_data),
                "potential_savings_gb": sum(f.get("size", 0) for f in cold_data) / (1024 ** 3) * 0.7,
                "description": "Migrate cold data to cheaper storage tiers"
            })
        
        if efficiency_analysis["small_files_count"] > 0:
            optimizations.append({
                "type": "small_file_consolidation",
                "priority": "high",
                "impact": "medium",
                "affected_files": efficiency_analysis["small_files_count"],
                "potential_savings_gb": efficiency_analysis["small_files_count"] * 0.001,  # Metadata savings
                "description": "Consolidate small files to reduce metadata overhead"
            })
        
        if orphaned_files:
            optimizations.append({
                "type": "orphaned_file_cleanup",
                "priority": "medium",
                "impact": "medium",
                "affected_files": len(orphaned_files),
                "potential_savings_gb": sum(f.get("size", 0) for f in orphaned_files) / (1024 ** 3),
                "description": "Remove orphaned temporary files"
            })
        
        if efficiency_analysis["over_replicated_count"] > 0:
            optimizations.append({
                "type": "replication_optimization",
                "priority": "medium",
                "impact": "high",
                "affected_files": efficiency_analysis["over_replicated_count"],
                "potential_savings_gb": waste_analysis["replication_waste_gb"],
                "description": "Optimize replication factors to reduce storage waste"
            })
        
        # Sort by priority and impact
        priority_order = {"high": 3, "medium": 2, "low": 1}
        optimizations.sort(key=lambda x: (priority_order[x["priority"]], priority_order[x["impact"]]), reverse=True)
        
        return optimizations