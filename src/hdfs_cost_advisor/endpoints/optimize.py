import logging
from typing import Dict, Any, List
from datetime import datetime
import uuid
from ..hdfs.client import HDFSClient
from ..llm.client import LLMClient
from ..cost.calculator import CostCalculator
from .scan import get_scan_results
from .generate_script import store_optimization_plan

logger = logging.getLogger(__name__)

# In-memory storage for optimization results
optimization_results_storage = {}

async def generate_recommendations(scan_id: str, hdfs_client: HDFSClient, 
                                 llm_client: LLMClient, cost_calculator: CostCalculator) -> Dict[str, Any]:
    """Generate cost optimization recommendations based on scan results"""
    
    optimization_id = str(uuid.uuid4())
    
    try:
        logger.info(f"Starting optimization analysis {optimization_id} for scan {scan_id}")
        
        # Get scan results
        scan_results = get_scan_results(scan_id)
        
        if scan_results.get("status") != "completed":
            raise ValueError(f"Scan {scan_id} is not completed or failed")
        
        # Calculate current costs
        current_costs = cost_calculator.calculate_current_costs(scan_results)
        
        # Generate LLM analysis
        llm_analysis = await llm_client.analyze_hdfs_cost_optimization(scan_results)
        
        # Create optimization plan from LLM recommendations
        optimization_plan = _create_optimization_plan(
            scan_results, llm_analysis, current_costs, cost_calculator
        )
        
        # Calculate detailed savings
        optimization_savings = cost_calculator.calculate_optimization_savings(
            scan_results, optimization_plan.get("optimizations", [])
        )
        
        # Generate cost report
        cost_report = cost_calculator.generate_cost_report(
            scan_results, optimization_plan.get("optimizations", [])
        )
        
        # Create comprehensive optimization results
        optimization_results = {
            "optimization_id": optimization_id,
            "scan_id": scan_id,
            "status": "completed",
            "created_at": datetime.utcnow().isoformat(),
            
            # LLM Analysis
            "llm_analysis": llm_analysis,
            
            # Current costs
            "current_costs": current_costs,
            
            # Optimization plan
            "optimization_plan": optimization_plan,
            
            # Savings analysis
            "optimization_savings": [
                {
                    "category": saving.category,
                    "current_cost": saving.current_cost,
                    "optimized_cost": saving.optimized_cost,
                    "monthly_savings": saving.savings,
                    "annual_savings": saving.annual_savings,
                    "savings_percent": saving.savings_percent,
                    "affected_data_gb": saving.affected_data_gb,
                    "implementation_cost": saving.implementation_cost
                }
                for saving in optimization_savings
            ],
            
            # Cost report
            "cost_report": cost_report,
            
            # Summary metrics
            "summary": {
                "total_monthly_savings": sum(s.savings for s in optimization_savings),
                "total_annual_savings": sum(s.annual_savings for s in optimization_savings),
                "total_implementation_cost": sum(s.implementation_cost for s in optimization_savings),
                "roi_months": _calculate_roi_months(optimization_savings),
                "affected_data_gb": sum(s.affected_data_gb for s in optimization_savings),
                "optimization_categories": list(set(s.category for s in optimization_savings))
            }
        }
        
        # Store results
        optimization_results_storage[optimization_id] = optimization_results
        
        # Store optimization plan for script generation
        store_optimization_plan(optimization_id, optimization_plan)
        
        logger.info(f"Optimization analysis {optimization_id} completed successfully")
        return optimization_results
        
    except Exception as e:
        logger.error(f"Optimization analysis {optimization_id} failed: {e}")
        error_result = {
            "optimization_id": optimization_id,
            "scan_id": scan_id,
            "status": "failed",
            "error": str(e),
            "created_at": datetime.utcnow().isoformat()
        }
        
        optimization_results_storage[optimization_id] = error_result
        raise

def _create_optimization_plan(scan_results: Dict[str, Any], llm_analysis: Dict[str, Any],
                            current_costs: Dict[str, Any], cost_calculator: CostCalculator) -> Dict[str, Any]:
    """Create detailed optimization plan from LLM analysis and scan results"""
    
    plan_id = str(uuid.uuid4())
    
    # Extract optimizations from LLM analysis
    llm_recommendations = llm_analysis.get("recommendations", [])
    
    # Create detailed optimization actions
    optimizations = []
    
    for recommendation in llm_recommendations:
        category = recommendation.get("category", "unknown")
        
        if category == "cold_data":
            optimization = _create_cold_data_optimization(scan_results, recommendation)
        elif category == "small_files":
            optimization = _create_small_files_optimization(scan_results, recommendation)
        elif category == "replication":
            optimization = _create_replication_optimization(scan_results, recommendation)
        elif category == "cleanup":
            optimization = _create_cleanup_optimization(scan_results, recommendation)
        else:
            # Generic optimization
            optimization = _create_generic_optimization(scan_results, recommendation)
        
        if optimization:
            optimizations.append(optimization)
    
    # Calculate totals
    total_monthly_savings = sum(opt.get("estimated_monthly_savings", 0) for opt in optimizations)
    total_annual_savings = total_monthly_savings * 12
    affected_data_gb = sum(opt.get("affected_data_gb", 0) for opt in optimizations)
    
    return {
        "plan_id": plan_id,
        "optimizations": optimizations,
        "total_monthly_savings": total_monthly_savings,
        "total_annual_savings": total_annual_savings,
        "affected_data_gb": affected_data_gb,
        "created_at": datetime.utcnow().isoformat(),
        "estimated_implementation_time": _estimate_implementation_time(optimizations)
    }

def _create_cold_data_optimization(scan_results: Dict[str, Any], recommendation: Dict[str, Any]) -> Dict[str, Any]:
    """Create cold data optimization actions"""
    cold_data = scan_results.get("cold_data", [])
    
    # Select files for cold storage migration
    files_to_migrate = []
    for file_info in cold_data:
        if file_info.get("days_since_access", 0) > 90:  # Files not accessed in 90+ days
            files_to_migrate.append({
                "path": file_info.get("path"),
                "size": file_info.get("size", 0),
                "size_gb": file_info.get("size", 0) / (1024 ** 3),
                "days_since_access": file_info.get("days_since_access", 0),
                "current_storage_policy": "HOT"
            })
    
    if not files_to_migrate:
        return None
    
    return {
        "category": "cold_data",
        "title": recommendation.get("title", "Cold Data Migration"),
        "description": recommendation.get("description", "Migrate cold data to cheaper storage"),
        "files": files_to_migrate,
        "estimated_monthly_savings": recommendation.get("estimated_savings_gb", 0) * 0.03,  # $0.03 savings per GB
        "affected_data_gb": sum(f["size_gb"] for f in files_to_migrate),
        "implementation_complexity": recommendation.get("implementation_complexity", "medium"),
        "timeline": recommendation.get("timeline", "1-2 weeks")
    }

def _create_small_files_optimization(scan_results: Dict[str, Any], recommendation: Dict[str, Any]) -> Dict[str, Any]:
    """Create small files consolidation actions"""
    small_files = scan_results.get("small_files", [])
    
    # Group small files by directory
    directory_groups = {}
    for file_info in small_files:
        path = file_info.get("path", "")
        directory = "/".join(path.split("/")[:-1])
        
        if directory not in directory_groups:
            directory_groups[directory] = []
        
        directory_groups[directory].append({
            "path": path,
            "size": file_info.get("size", 0),
            "size_gb": file_info.get("size", 0) / (1024 ** 3)
        })
    
    # Select directories with many small files
    directories_to_consolidate = []
    for directory, files in directory_groups.items():
        if len(files) >= 10:  # Directories with 10+ small files
            directories_to_consolidate.append({
                "path": directory,
                "small_files": files,
                "file_count": len(files),
                "total_size_gb": sum(f["size_gb"] for f in files)
            })
    
    if not directories_to_consolidate:
        return None
    
    return {
        "category": "small_files",
        "title": recommendation.get("title", "Small Files Consolidation"),
        "description": recommendation.get("description", "Consolidate small files to reduce overhead"),
        "directories": directories_to_consolidate,
        "estimated_monthly_savings": len(small_files) * 0.001,  # $0.001 per small file
        "affected_data_gb": sum(d["total_size_gb"] for d in directories_to_consolidate),
        "implementation_complexity": recommendation.get("implementation_complexity", "high"),
        "timeline": recommendation.get("timeline", "1 month")
    }

def _create_replication_optimization(scan_results: Dict[str, Any], recommendation: Dict[str, Any]) -> Dict[str, Any]:
    """Create replication optimization actions"""
    over_replicated = scan_results.get("over_replicated_files", [])
    
    files_to_optimize = []
    for file_info in over_replicated:
        files_to_optimize.append({
            "path": file_info.get("path"),
            "size": file_info.get("size", 0),
            "size_gb": file_info.get("size", 0) / (1024 ** 3),
            "current_replication": file_info.get("current_replication", 3),
            "suggested_replication": file_info.get("suggested_replication", 3)
        })
    
    if not files_to_optimize:
        return None
    
    return {
        "category": "replication",
        "title": recommendation.get("title", "Replication Optimization"),
        "description": recommendation.get("description", "Optimize replication factors"),
        "files": files_to_optimize,
        "estimated_monthly_savings": recommendation.get("estimated_savings_gb", 0) * 0.04,  # $0.04 per GB saved
        "affected_data_gb": sum(f["size_gb"] for f in files_to_optimize),
        "implementation_complexity": recommendation.get("implementation_complexity", "low"),
        "timeline": recommendation.get("timeline", "immediate")
    }

def _create_cleanup_optimization(scan_results: Dict[str, Any], recommendation: Dict[str, Any]) -> Dict[str, Any]:
    """Create cleanup optimization actions"""
    orphaned_files = scan_results.get("orphaned_files", [])
    empty_files = scan_results.get("empty_files", [])
    
    files_to_delete = []
    
    # Add orphaned files
    for file_info in orphaned_files:
        files_to_delete.append({
            "path": file_info.get("path"),
            "size": file_info.get("size", 0),
            "size_gb": file_info.get("size", 0) / (1024 ** 3),
            "type": "orphaned",
            "age_days": file_info.get("age_days", 0),
            "cleanup_priority": file_info.get("cleanup_priority", "medium")
        })
    
    # Add empty files
    for file_info in empty_files:
        files_to_delete.append({
            "path": file_info.get("path"),
            "size": file_info.get("size", 0),
            "size_gb": 0,
            "type": "empty",
            "cleanup_priority": "low"
        })
    
    if not files_to_delete:
        return None
    
    return {
        "category": "cleanup",
        "title": recommendation.get("title", "File Cleanup"),
        "description": recommendation.get("description", "Remove unnecessary files"),
        "files": files_to_delete,
        "estimated_monthly_savings": sum(f["size_gb"] for f in files_to_delete) * 0.04 * 3,  # Full storage savings
        "affected_data_gb": sum(f["size_gb"] for f in files_to_delete),
        "implementation_complexity": recommendation.get("implementation_complexity", "low"),
        "timeline": recommendation.get("timeline", "immediate")
    }

def _create_generic_optimization(scan_results: Dict[str, Any], recommendation: Dict[str, Any]) -> Dict[str, Any]:
    """Create generic optimization from LLM recommendation"""
    return {
        "category": recommendation.get("category", "generic"),
        "title": recommendation.get("title", "Generic Optimization"),
        "description": recommendation.get("description", ""),
        "estimated_monthly_savings": recommendation.get("estimated_savings_gb", 0) * 0.04,
        "affected_data_gb": recommendation.get("estimated_savings_gb", 0),
        "implementation_complexity": recommendation.get("implementation_complexity", "medium"),
        "timeline": recommendation.get("timeline", "1-2 weeks"),
        "steps": recommendation.get("steps", [])
    }

def _calculate_roi_months(optimization_savings: List[Any]) -> float:
    """Calculate ROI in months"""
    total_savings = sum(s.savings for s in optimization_savings)
    total_implementation_cost = sum(s.implementation_cost for s in optimization_savings)
    
    if total_savings <= 0:
        return float('inf')
    
    return total_implementation_cost / total_savings

def _estimate_implementation_time(optimizations: List[Dict[str, Any]]) -> str:
    """Estimate total implementation time"""
    complexity_weights = {
        "low": 1,
        "medium": 2,
        "high": 3
    }
    
    total_complexity = sum(
        complexity_weights.get(opt.get("implementation_complexity", "medium"), 2)
        for opt in optimizations
    )
    
    if total_complexity <= 3:
        return "1-2 weeks"
    elif total_complexity <= 6:
        return "1 month"
    else:
        return "2-3 months"

def get_optimization_results(optimization_id: str) -> Dict[str, Any]:
    """Retrieve optimization results by ID"""
    if optimization_id not in optimization_results_storage:
        raise ValueError(f"Optimization results not found for ID: {optimization_id}")
    
    return optimization_results_storage[optimization_id]

def list_optimizations() -> List[Dict[str, Any]]:
    """List all optimization analyses"""
    return [
        {
            "optimization_id": opt_id,
            "scan_id": results.get("scan_id"),
            "status": results.get("status"),
            "created_at": results.get("created_at"),
            "total_monthly_savings": results.get("summary", {}).get("total_monthly_savings", 0),
            "total_annual_savings": results.get("summary", {}).get("total_annual_savings", 0),
            "affected_data_gb": results.get("summary", {}).get("affected_data_gb", 0)
        }
        for opt_id, results in optimization_results_storage.items()
    ]