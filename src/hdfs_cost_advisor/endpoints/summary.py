import logging
from typing import Dict, Any, List
from datetime import datetime
from ..cost.calculator import CostCalculator
from .scan import get_scan_results
from .optimize import get_optimization_results

logger = logging.getLogger(__name__)

def generate_summary(scan_id: str, cost_calculator: CostCalculator) -> Dict[str, Any]:
    """Generate comprehensive summary of scan results and potential savings"""
    
    try:
        logger.info(f"Generating summary for scan {scan_id}")
        
        # Get scan results
        scan_results = get_scan_results(scan_id)
        
        if scan_results.get("status") != "completed":
            raise ValueError(f"Scan {scan_id} is not completed or failed")
        
        # Calculate current costs
        current_costs = cost_calculator.calculate_current_costs(scan_results)
        
        # Generate summary
        summary = {
            "scan_id": scan_id,
            "generated_at": datetime.utcnow().isoformat(),
            "status": "completed",
            
            # Basic scan information
            "scan_info": {
                "scan_completed": scan_results.get("scan_completed"),
                "scanned_paths": scan_results.get("scanned_paths", []),
                "scan_depth": scan_results.get("scan_depth", 0),
                "total_files": scan_results.get("total_files", 0),
                "total_size_gb": scan_results.get("total_size_gb", 0),
                "total_size_tb": scan_results.get("total_size_gb", 0) / 1024
            },
            
            # Current costs
            "current_costs": current_costs,
            
            # Optimization opportunities
            "optimization_opportunities": _analyze_optimization_opportunities(scan_results),
            
            # Efficiency metrics
            "efficiency_metrics": _calculate_efficiency_metrics(scan_results),
            
            # Storage waste analysis
            "waste_analysis": scan_results.get("waste_analysis", {}),
            
            # Cluster health
            "cluster_health": _analyze_cluster_health(scan_results),
            
            # Risk assessment
            "risk_assessment": _assess_risks(scan_results),
            
            # Recommendations summary
            "recommendations_summary": _generate_recommendations_summary(scan_results, cost_calculator),
            
            # Projected savings
            "projected_savings": _calculate_projected_savings(scan_results, cost_calculator)
        }
        
        logger.info(f"Summary generated successfully for scan {scan_id}")
        return summary
        
    except Exception as e:
        logger.error(f"Failed to generate summary for scan {scan_id}: {e}")
        return {
            "scan_id": scan_id,
            "status": "failed",
            "error": str(e),
            "generated_at": datetime.utcnow().isoformat()
        }

def _analyze_optimization_opportunities(scan_results: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze available optimization opportunities"""
    
    cold_data = scan_results.get("cold_data", [])
    small_files = scan_results.get("small_files", [])
    empty_files = scan_results.get("empty_files", [])
    orphaned_files = scan_results.get("orphaned_files", [])
    over_replicated = scan_results.get("over_replicated_files", [])
    duplicates = scan_results.get("duplicate_candidates", [])
    
    # Calculate potential savings for each category
    cold_data_size_gb = sum(f.get("size", 0) for f in cold_data) / (1024 ** 3)
    orphaned_size_gb = sum(f.get("size", 0) for f in orphaned_files) / (1024 ** 3)
    over_replicated_size_gb = sum(f.get("size", 0) for f in over_replicated) / (1024 ** 3)
    
    return {
        "cold_data_migration": {
            "file_count": len(cold_data),
            "size_gb": cold_data_size_gb,
            "potential_monthly_savings": cold_data_size_gb * 0.03,  # $0.03 per GB savings
            "priority": "high" if cold_data_size_gb > 100 else "medium"
        },
        "small_file_consolidation": {
            "file_count": len(small_files),
            "size_gb": sum(f.get("size", 0) for f in small_files) / (1024 ** 3),
            "potential_monthly_savings": len(small_files) * 0.001,  # $0.001 per small file
            "priority": "high" if len(small_files) > 10000 else "medium"
        },
        "file_cleanup": {
            "orphaned_files": len(orphaned_files),
            "empty_files": len(empty_files),
            "size_gb": orphaned_size_gb,
            "potential_monthly_savings": orphaned_size_gb * 0.04 * 3,  # Full storage cost
            "priority": "medium"
        },
        "replication_optimization": {
            "file_count": len(over_replicated),
            "size_gb": over_replicated_size_gb,
            "potential_monthly_savings": over_replicated_size_gb * 0.04,  # $0.04 per GB
            "priority": "low"
        },
        "duplicate_removal": {
            "file_count": len(duplicates),
            "size_gb": sum(f.get("size", 0) for f in duplicates) / (1024 ** 3),
            "potential_monthly_savings": sum(f.get("size", 0) for f in duplicates) / (1024 ** 3) * 0.02,
            "priority": "low"
        }
    }

def _calculate_efficiency_metrics(scan_results: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate storage efficiency metrics"""
    
    total_files = scan_results.get("total_files", 0)
    total_size_gb = scan_results.get("total_size_gb", 0)
    
    efficiency_analysis = scan_results.get("efficiency_analysis", {})
    
    # Calculate average file size
    avg_file_size_mb = (total_size_gb * 1024) / total_files if total_files > 0 else 0
    
    # Storage efficiency score (0-100)
    small_file_penalty = min(efficiency_analysis.get("small_files_percentage", 0), 50)
    over_replication_penalty = min(efficiency_analysis.get("over_replicated_percentage", 0), 30)
    
    efficiency_score = max(0, 100 - small_file_penalty - over_replication_penalty)
    
    return {
        "average_file_size_mb": avg_file_size_mb,
        "efficiency_score": efficiency_score,
        "small_files_percentage": efficiency_analysis.get("small_files_percentage", 0),
        "over_replicated_percentage": efficiency_analysis.get("over_replicated_percentage", 0),
        "empty_files_count": efficiency_analysis.get("empty_files_count", 0),
        "storage_utilization": {
            "optimal_range": "64MB - 1GB per file",
            "current_average": f"{avg_file_size_mb:.1f}MB",
            "recommendation": _get_file_size_recommendation(avg_file_size_mb)
        }
    }

def _analyze_cluster_health(scan_results: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze cluster health metrics"""
    
    cluster_metrics = scan_results.get("cluster_metrics", {})
    filesystem_metrics = cluster_metrics.get("filesystem", {})
    
    capacity_total = filesystem_metrics.get("capacity_total", 0)
    capacity_used = filesystem_metrics.get("capacity_used", 0)
    capacity_remaining = filesystem_metrics.get("capacity_remaining", 0)
    
    # Calculate utilization percentage
    utilization_percent = (capacity_used / capacity_total) * 100 if capacity_total > 0 else 0
    
    # Determine health status
    if utilization_percent < 70:
        health_status = "healthy"
    elif utilization_percent < 85:
        health_status = "warning"
    else:
        health_status = "critical"
    
    return {
        "health_status": health_status,
        "capacity_utilization_percent": utilization_percent,
        "capacity_total_gb": capacity_total / (1024 ** 3),
        "capacity_used_gb": capacity_used / (1024 ** 3),
        "capacity_remaining_gb": capacity_remaining / (1024 ** 3),
        "under_replicated_blocks": filesystem_metrics.get("under_replicated_blocks", 0),
        "corrupt_blocks": filesystem_metrics.get("corrupt_blocks", 0),
        "files_total": filesystem_metrics.get("files_total", 0),
        "blocks_total": filesystem_metrics.get("blocks_total", 0)
    }

def _assess_risks(scan_results: Dict[str, Any]) -> Dict[str, Any]:
    """Assess risks associated with current storage state"""
    
    cluster_health = _analyze_cluster_health(scan_results)
    efficiency_metrics = _calculate_efficiency_metrics(scan_results)
    
    risks = []
    
    # High utilization risk
    if cluster_health["capacity_utilization_percent"] > 85:
        risks.append({
            "type": "high_utilization",
            "severity": "critical",
            "description": "Cluster utilization is critically high",
            "recommendation": "Immediate cleanup or capacity expansion required"
        })
    
    # Small files risk
    if efficiency_metrics["small_files_percentage"] > 50:
        risks.append({
            "type": "small_files",
            "severity": "high",
            "description": "High percentage of small files causing metadata overhead",
            "recommendation": "Implement file consolidation strategy"
        })
    
    # Corrupt blocks risk
    if cluster_health["corrupt_blocks"] > 0:
        risks.append({
            "type": "data_corruption",
            "severity": "critical",
            "description": f"{cluster_health['corrupt_blocks']} corrupt blocks detected",
            "recommendation": "Immediate data recovery required"
        })
    
    # Under-replicated blocks risk
    if cluster_health["under_replicated_blocks"] > 0:
        risks.append({
            "type": "under_replication",
            "severity": "medium",
            "description": f"{cluster_health['under_replicated_blocks']} under-replicated blocks",
            "recommendation": "Check cluster health and replication settings"
        })
    
    # Calculate overall risk score
    risk_score = sum({
        "critical": 10,
        "high": 5,
        "medium": 2,
        "low": 1
    }.get(risk["severity"], 0) for risk in risks)
    
    return {
        "overall_risk_score": risk_score,
        "risk_level": "critical" if risk_score >= 10 else "high" if risk_score >= 5 else "medium" if risk_score >= 2 else "low",
        "risks": risks,
        "recommendations": [risk["recommendation"] for risk in risks]
    }

def _generate_recommendations_summary(scan_results: Dict[str, Any], cost_calculator: CostCalculator) -> Dict[str, Any]:
    """Generate high-level recommendations summary"""
    
    opportunities = _analyze_optimization_opportunities(scan_results)
    efficiency = _calculate_efficiency_metrics(scan_results)
    
    recommendations = []
    
    # Prioritize recommendations based on potential savings
    if opportunities["cold_data_migration"]["potential_monthly_savings"] > 100:
        recommendations.append({
            "priority": 1,
            "action": "Cold Data Migration",
            "description": f"Migrate {opportunities['cold_data_migration']['file_count']} files to cold storage",
            "estimated_monthly_savings": opportunities["cold_data_migration"]["potential_monthly_savings"],
            "timeline": "1-2 weeks"
        })
    
    if opportunities["small_file_consolidation"]["file_count"] > 5000:
        recommendations.append({
            "priority": 2,
            "action": "Small File Consolidation",
            "description": f"Consolidate {opportunities['small_file_consolidation']['file_count']} small files",
            "estimated_monthly_savings": opportunities["small_file_consolidation"]["potential_monthly_savings"],
            "timeline": "2-4 weeks"
        })
    
    if opportunities["file_cleanup"]["potential_monthly_savings"] > 50:
        recommendations.append({
            "priority": 3,
            "action": "File Cleanup",
            "description": f"Remove {opportunities['file_cleanup']['orphaned_files']} orphaned and {opportunities['file_cleanup']['empty_files']} empty files",
            "estimated_monthly_savings": opportunities["file_cleanup"]["potential_monthly_savings"],
            "timeline": "Immediate"
        })
    
    return {
        "total_recommendations": len(recommendations),
        "recommendations": recommendations,
        "estimated_total_monthly_savings": sum(r["estimated_monthly_savings"] for r in recommendations),
        "estimated_total_annual_savings": sum(r["estimated_monthly_savings"] for r in recommendations) * 12
    }

def _calculate_projected_savings(scan_results: Dict[str, Any], cost_calculator: CostCalculator) -> Dict[str, Any]:
    """Calculate projected savings from optimizations"""
    
    current_costs = cost_calculator.calculate_current_costs(scan_results)
    opportunities = _analyze_optimization_opportunities(scan_results)
    
    # Calculate potential savings by category
    monthly_savings = {
        "cold_data": opportunities["cold_data_migration"]["potential_monthly_savings"],
        "small_files": opportunities["small_file_consolidation"]["potential_monthly_savings"],
        "cleanup": opportunities["file_cleanup"]["potential_monthly_savings"],
        "replication": opportunities["replication_optimization"]["potential_monthly_savings"],
        "duplicates": opportunities["duplicate_removal"]["potential_monthly_savings"]
    }
    
    total_monthly_savings = sum(monthly_savings.values())
    total_annual_savings = total_monthly_savings * 12
    
    # Calculate ROI
    current_monthly_cost = current_costs["total_monthly_cost"]
    savings_percentage = (total_monthly_savings / current_monthly_cost) * 100 if current_monthly_cost > 0 else 0
    
    return {
        "current_monthly_cost": current_monthly_cost,
        "projected_monthly_savings": total_monthly_savings,
        "projected_annual_savings": total_annual_savings,
        "savings_percentage": savings_percentage,
        "optimized_monthly_cost": current_monthly_cost - total_monthly_savings,
        "savings_by_category": monthly_savings,
        "payback_period": "immediate",  # Most optimizations have immediate payback
        "confidence_level": "high" if savings_percentage > 20 else "medium" if savings_percentage > 10 else "low"
    }

def _get_file_size_recommendation(avg_file_size_mb: float) -> str:
    """Get file size recommendation based on average"""
    if avg_file_size_mb < 1:
        return "Consider consolidating small files"
    elif avg_file_size_mb < 64:
        return "File sizes are below optimal range"
    elif avg_file_size_mb <= 1024:
        return "File sizes are in optimal range"
    else:
        return "Consider splitting large files"

def get_optimization_summary(optimization_id: str) -> Dict[str, Any]:
    """Get summary of optimization results"""
    
    try:
        optimization_results = get_optimization_results(optimization_id)
        
        return {
            "optimization_id": optimization_id,
            "scan_id": optimization_results.get("scan_id"),
            "status": optimization_results.get("status"),
            "created_at": optimization_results.get("created_at"),
            "summary": optimization_results.get("summary", {}),
            "cost_report": optimization_results.get("cost_report", {}),
            "implementation_plan": {
                "total_actions": len(optimization_results.get("optimization_plan", {}).get("optimizations", [])),
                "estimated_time": optimization_results.get("optimization_plan", {}).get("estimated_implementation_time", "Unknown"),
                "categories": optimization_results.get("summary", {}).get("optimization_categories", [])
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get optimization summary for {optimization_id}: {e}")
        return {
            "optimization_id": optimization_id,
            "status": "failed",
            "error": str(e)
        }