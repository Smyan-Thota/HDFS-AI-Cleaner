from typing import Dict, Any, List
from dataclasses import dataclass
import logging

@dataclass
class StorageCosts:
    standard_storage_cost_per_gb: float = 0.04  # $/GB/month
    cold_storage_cost_per_gb: float = 0.01      # $/GB/month
    archive_storage_cost_per_gb: float = 0.005  # $/GB/month
    replication_multiplier: float = 1.0
    metadata_cost_per_file: float = 0.0001      # $/file/month
    network_cost_per_gb: float = 0.01           # $/GB for data movement

@dataclass
class OptimizationSavings:
    category: str
    current_cost: float
    optimized_cost: float
    savings: float
    savings_percent: float
    affected_data_gb: float
    implementation_cost: float = 0.0
    annual_savings: float = 0.0

class CostCalculator:
    def __init__(self, storage_costs: StorageCosts):
        self.costs = storage_costs
        self.logger = logging.getLogger(__name__)
    
    def calculate_current_costs(self, scan_results: Dict[str, Any]) -> Dict[str, float]:
        """Calculate current HDFS storage costs"""
        total_size_gb = scan_results.get("total_size_gb", 0)
        total_files = scan_results.get("total_files", 0)
        
        # Calculate storage costs (assuming 3x replication)
        replication_factor = 3
        storage_cost = total_size_gb * self.costs.standard_storage_cost_per_gb * replication_factor
        
        # Calculate metadata costs
        metadata_cost = total_files * self.costs.metadata_cost_per_file
        
        # Calculate additional costs for small files (metadata overhead)
        small_files = scan_results.get("small_files", [])
        small_file_overhead = len(small_files) * 0.001  # Additional cost per small file
        
        # Calculate network costs (data movement)
        network_cost = total_size_gb * 0.005  # Estimated network usage
        
        total_cost = storage_cost + metadata_cost + small_file_overhead + network_cost
        
        return {
            "storage_cost": storage_cost,
            "metadata_cost": metadata_cost,
            "small_file_overhead": small_file_overhead,
            "network_cost": network_cost,
            "total_monthly_cost": total_cost,
            "total_annual_cost": total_cost * 12,
            "cost_per_gb": total_cost / total_size_gb if total_size_gb > 0 else 0
        }
    
    def calculate_optimization_savings(self, scan_results: Dict[str, Any], 
                                     optimizations: List[Dict[str, Any]]) -> List[OptimizationSavings]:
        """Calculate potential savings from each optimization"""
        savings_list = []
        
        for optimization in optimizations:
            category = optimization.get("category", "unknown")
            
            if category == "cold_data":
                savings = self._calculate_cold_data_savings(scan_results, optimization)
            elif category == "small_files":
                savings = self._calculate_small_file_savings(scan_results, optimization)
            elif category == "replication":
                savings = self._calculate_replication_savings(scan_results, optimization)
            elif category == "cleanup":
                savings = self._calculate_cleanup_savings(scan_results, optimization)
            elif category == "compression":
                savings = self._calculate_compression_savings(scan_results, optimization)
            else:
                continue
            
            savings_list.append(savings)
        
        return savings_list
    
    def _calculate_cold_data_savings(self, scan_results: Dict[str, Any], 
                                   optimization: Dict[str, Any]) -> OptimizationSavings:
        """Calculate savings from cold data optimization"""
        cold_data = scan_results.get("cold_data", [])
        cold_data_size_gb = sum(file.get("size", 0) for file in cold_data) / (1024 ** 3)
        
        # Current cost (standard storage with 3x replication)
        current_cost = cold_data_size_gb * self.costs.standard_storage_cost_per_gb * 3
        
        # Optimized cost (cold storage with 1.5x replication)
        optimized_cost = cold_data_size_gb * self.costs.cold_storage_cost_per_gb * 1.5
        
        # Implementation cost (data movement)
        implementation_cost = cold_data_size_gb * self.costs.network_cost_per_gb
        
        savings = current_cost - optimized_cost
        savings_percent = (savings / current_cost) * 100 if current_cost > 0 else 0
        
        return OptimizationSavings(
            category="cold_data",
            current_cost=current_cost,
            optimized_cost=optimized_cost,
            savings=savings,
            savings_percent=savings_percent,
            affected_data_gb=cold_data_size_gb,
            implementation_cost=implementation_cost,
            annual_savings=savings * 12
        )
    
    def _calculate_small_file_savings(self, scan_results: Dict[str, Any], 
                                    optimization: Dict[str, Any]) -> OptimizationSavings:
        """Calculate savings from small file consolidation"""
        small_files = scan_results.get("small_files", [])
        small_file_count = len(small_files)
        small_file_size_gb = sum(file.get("size", 0) for file in small_files) / (1024 ** 3)
        
        # Current metadata overhead (100x normal metadata cost)
        current_metadata_cost = small_file_count * self.costs.metadata_cost_per_file * 100
        
        # Current storage cost
        current_storage_cost = small_file_size_gb * self.costs.standard_storage_cost_per_gb * 3
        current_cost = current_metadata_cost + current_storage_cost
        
        # Optimized cost (after consolidation - assume 90% reduction in file count)
        optimized_file_count = small_file_count * 0.1
        optimized_metadata_cost = optimized_file_count * self.costs.metadata_cost_per_file * 100
        optimized_storage_cost = small_file_size_gb * self.costs.standard_storage_cost_per_gb * 3
        optimized_cost = optimized_metadata_cost + optimized_storage_cost
        
        # Implementation cost (processing overhead)
        implementation_cost = small_file_count * 0.0001  # $0.0001 per file to process
        
        savings = current_cost - optimized_cost
        savings_percent = (savings / current_cost) * 100 if current_cost > 0 else 0
        
        return OptimizationSavings(
            category="small_files",
            current_cost=current_cost,
            optimized_cost=optimized_cost,
            savings=savings,
            savings_percent=savings_percent,
            affected_data_gb=small_file_size_gb,
            implementation_cost=implementation_cost,
            annual_savings=savings * 12
        )
    
    def _calculate_replication_savings(self, scan_results: Dict[str, Any], 
                                     optimization: Dict[str, Any]) -> OptimizationSavings:
        """Calculate savings from replication optimization"""
        # Look for over-replicated files in efficiency analysis
        efficiency_analysis = scan_results.get("efficiency_analysis", {})
        over_replicated = efficiency_analysis.get("inefficient_replication", [])
        
        total_savings = 0
        total_size_gb = 0
        
        for file_info in over_replicated:
            size_gb = file_info.get("size", 0) / (1024 ** 3)
            current_replication = file_info.get("current_replication", 3)
            suggested_replication = file_info.get("suggested_replication", 3)
            
            # Calculate savings from reducing replication
            excess_replicas = current_replication - suggested_replication
            file_savings = size_gb * self.costs.standard_storage_cost_per_gb * excess_replicas
            total_savings += file_savings
            total_size_gb += size_gb
        
        current_cost = total_size_gb * self.costs.standard_storage_cost_per_gb * 4  # Assume avg 4x replication
        optimized_cost = total_size_gb * self.costs.standard_storage_cost_per_gb * 3  # Reduce to 3x
        
        savings = current_cost - optimized_cost
        savings_percent = (savings / current_cost) * 100 if current_cost > 0 else 0
        
        return OptimizationSavings(
            category="replication",
            current_cost=current_cost,
            optimized_cost=optimized_cost,
            savings=savings,
            savings_percent=savings_percent,
            affected_data_gb=total_size_gb,
            implementation_cost=0,  # No implementation cost for replication changes
            annual_savings=savings * 12
        )
    
    def _calculate_cleanup_savings(self, scan_results: Dict[str, Any], 
                                 optimization: Dict[str, Any]) -> OptimizationSavings:
        """Calculate savings from cleanup operations"""
        orphaned_files = scan_results.get("orphaned_files", [])
        orphaned_size_gb = sum(file.get("size", 0) for file in orphaned_files) / (1024 ** 3)
        
        # Current cost (storage + metadata for orphaned files)
        current_storage_cost = orphaned_size_gb * self.costs.standard_storage_cost_per_gb * 3
        current_metadata_cost = len(orphaned_files) * self.costs.metadata_cost_per_file
        current_cost = current_storage_cost + current_metadata_cost
        
        # Optimized cost (after cleanup)
        optimized_cost = 0  # Files are deleted
        
        savings = current_cost - optimized_cost
        savings_percent = 100.0  # 100% savings from deleted files
        
        return OptimizationSavings(
            category="cleanup",
            current_cost=current_cost,
            optimized_cost=optimized_cost,
            savings=savings,
            savings_percent=savings_percent,
            affected_data_gb=orphaned_size_gb,
            implementation_cost=0,  # Minimal cost for cleanup
            annual_savings=savings * 12
        )
    
    def _calculate_compression_savings(self, scan_results: Dict[str, Any], 
                                     optimization: Dict[str, Any]) -> OptimizationSavings:
        """Calculate savings from data compression"""
        total_size_gb = scan_results.get("total_size_gb", 0)
        
        # Assume 30% compression ratio for typical data
        compression_ratio = 0.3
        compressed_size_gb = total_size_gb * (1 - compression_ratio)
        
        # Current cost
        current_cost = total_size_gb * self.costs.standard_storage_cost_per_gb * 3
        
        # Optimized cost (after compression)
        optimized_cost = compressed_size_gb * self.costs.standard_storage_cost_per_gb * 3
        
        # Implementation cost (CPU cycles for compression)
        implementation_cost = total_size_gb * 0.002  # $0.002 per GB for compression
        
        savings = current_cost - optimized_cost
        savings_percent = (savings / current_cost) * 100 if current_cost > 0 else 0
        
        return OptimizationSavings(
            category="compression",
            current_cost=current_cost,
            optimized_cost=optimized_cost,
            savings=savings,
            savings_percent=savings_percent,
            affected_data_gb=total_size_gb,
            implementation_cost=implementation_cost,
            annual_savings=savings * 12
        )
    
    def generate_cost_report(self, scan_results: Dict[str, Any], 
                           optimizations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate comprehensive cost optimization report"""
        current_costs = self.calculate_current_costs(scan_results)
        optimization_savings = self.calculate_optimization_savings(scan_results, optimizations)
        
        total_savings = sum(saving.savings for saving in optimization_savings)
        total_implementation_cost = sum(saving.implementation_cost for saving in optimization_savings)
        total_annual_savings = sum(saving.annual_savings for saving in optimization_savings)
        total_current_cost = current_costs["total_monthly_cost"]
        
        # Calculate ROI
        roi_months = total_implementation_cost / total_savings if total_savings > 0 else float('inf')
        roi_percent = (total_annual_savings / total_implementation_cost) * 100 if total_implementation_cost > 0 else float('inf')
        
        return {
            "current_costs": current_costs,
            "optimization_breakdown": [
                {
                    "category": saving.category,
                    "monthly_savings": saving.savings,
                    "annual_savings": saving.annual_savings,
                    "savings_percent": saving.savings_percent,
                    "affected_data_gb": saving.affected_data_gb,
                    "implementation_cost": saving.implementation_cost,
                    "payback_months": saving.implementation_cost / saving.savings if saving.savings > 0 else float('inf')
                }
                for saving in optimization_savings
            ],
            "summary": {
                "total_monthly_savings": total_savings,
                "total_annual_savings": total_annual_savings,
                "total_implementation_cost": total_implementation_cost,
                "roi_percent": roi_percent,
                "payback_months": roi_months,
                "optimized_monthly_cost": total_current_cost - total_savings,
                "cost_reduction_percent": (total_savings / total_current_cost) * 100 if total_current_cost > 0 else 0
            }
        }
    
    def estimate_storage_growth(self, scan_results: Dict[str, Any], 
                              growth_rate_percent: float = 20) -> Dict[str, Any]:
        """Estimate future storage costs based on growth rate"""
        current_size_gb = scan_results.get("total_size_gb", 0)
        current_costs = self.calculate_current_costs(scan_results)
        
        # Project costs for next 3 years
        projections = []
        for year in range(1, 4):
            projected_size = current_size_gb * ((1 + growth_rate_percent / 100) ** year)
            projected_monthly_cost = (projected_size / current_size_gb) * current_costs["total_monthly_cost"]
            
            projections.append({
                "year": year,
                "projected_size_gb": projected_size,
                "projected_monthly_cost": projected_monthly_cost,
                "projected_annual_cost": projected_monthly_cost * 12
            })
        
        return {
            "current_size_gb": current_size_gb,
            "growth_rate_percent": growth_rate_percent,
            "projections": projections,
            "three_year_total_cost": sum(p["projected_annual_cost"] for p in projections)
        }