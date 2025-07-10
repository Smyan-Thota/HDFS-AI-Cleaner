from typing import Dict, Any, List
from jinja2 import Template
import logging
from datetime import datetime
import json
import os

class HDFSScriptGenerator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.script_storage = {}  # In-memory storage for demo
    
    def generate_optimization_script(self, optimization_plan: Dict[str, Any]) -> str:
        """Generate comprehensive HDFS optimization script"""
        
        script_template = Template("""#!/bin/bash
# HDFS Cost Optimization Script
# Generated: {{ timestamp }}
# Optimization Plan ID: {{ plan_id }}
# Total Estimated Savings: ${{ total_monthly_savings }}/month

set -e  # Exit on error
set -u  # Exit on undefined variable

# Configuration
LOG_FILE="/var/log/hdfs_optimization_$(date +%Y%m%d_%H%M%S).log"
BACKUP_DIR="/tmp/hdfs_backup_$(date +%Y%m%d_%H%M%S)"
DRY_RUN=${DRY_RUN:-false}

# Colors for output
RED='\\033[0;31m'
GREEN='\\033[0;32m'
YELLOW='\\033[1;33m'
NC='\\033[0m' # No Color

# Logging function
log() {
    local level=$1
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case $level in
        ERROR)
            echo -e "${RED}[$timestamp] ERROR: $message${NC}" | tee -a "$LOG_FILE"
            ;;
        WARN)
            echo -e "${YELLOW}[$timestamp] WARN: $message${NC}" | tee -a "$LOG_FILE"
            ;;
        INFO)
            echo -e "${GREEN}[$timestamp] INFO: $message${NC}" | tee -a "$LOG_FILE"
            ;;
        *)
            echo "[$timestamp] $message" | tee -a "$LOG_FILE"
            ;;
    esac
}

# Function to check if HDFS is accessible
check_hdfs_access() {
    log INFO "Checking HDFS access..."
    if ! hdfs dfs -test -d /; then
        log ERROR "Cannot access HDFS. Please check your configuration."
        exit 1
    fi
    log INFO "HDFS access confirmed"
}

# Function to create backup
create_backup() {
    log INFO "Creating backup directory: $BACKUP_DIR"
    mkdir -p "$BACKUP_DIR"
    
    # Create metadata backup
    hdfs dfsadmin -report > "$BACKUP_DIR/cluster_report_before.txt"
    hdfs fsck / -files -blocks > "$BACKUP_DIR/fsck_before.txt"
    
    log INFO "Backup created successfully"
}

# Function to execute command with dry-run support
execute_command() {
    local cmd="$1"
    local description="$2"
    
    log INFO "Executing: $description"
    
    if [ "$DRY_RUN" = "true" ]; then
        log INFO "[DRY RUN] Would execute: $cmd"
    else
        log INFO "Running: $cmd"
        eval "$cmd" || {
            log ERROR "Command failed: $cmd"
            return 1
        }
    fi
}

# Main execution starts here
log INFO "Starting HDFS cost optimization..."
log INFO "Optimization plan: {{ plan_id }}"
log INFO "Estimated monthly savings: ${{ total_monthly_savings }}"

# Check prerequisites
check_hdfs_access
create_backup

{% for optimization in optimizations %}
{% if optimization.category == "cold_data" %}
# ========================================
# Cold Data Optimization
# ========================================
log INFO "Starting cold data optimization..."
log INFO "Files to process: {{ optimization.files|length }}"

{% for file in optimization.files %}
# Processing: {{ file.path }}
log INFO "Moving {{ file.path }} to cold storage ({{ file.size_gb|round(2) }}GB)"

# Set cold storage policy
execute_command "hdfs storagepolicies -setStoragePolicy -path '{{ file.path }}' -policy COLD" \
    "Set cold storage policy for {{ file.path }}"

# Reduce replication factor
execute_command "hdfs dfs -setrep 1 '{{ file.path }}'" \
    "Reduce replication for {{ file.path }}"

# Verify storage policy
if [ "$DRY_RUN" = "false" ]; then
    POLICY=$(hdfs storagepolicies -getStoragePolicy -path '{{ file.path }}' | grep -o 'COLD\\|HOT\\|WARM')
    log INFO "Storage policy for {{ file.path }}: $POLICY"
fi

{% endfor %}
log INFO "Cold data optimization completed"
{% endif %}

{% if optimization.category == "small_files" %}
# ========================================
# Small Files Consolidation
# ========================================
log INFO "Starting small files consolidation..."
log INFO "Directories to process: {{ optimization.directories|length }}"

{% for directory in optimization.directories %}
# Processing directory: {{ directory.path }}
log INFO "Consolidating small files in {{ directory.path }}"

TEMP_FILE="/tmp/consolidated_$(basename '{{ directory.path }}')_$(date +%Y%m%d_%H%M%S)"
CONSOLIDATED_PATH="{{ directory.path }}/consolidated_$(date +%Y%m%d_%H%M%S)"

# Create consolidated file
execute_command "hdfs dfs -getmerge '{{ directory.path }}' '$TEMP_FILE'" \
    "Merge small files from {{ directory.path }}"

# Upload consolidated file
execute_command "hdfs dfs -put '$TEMP_FILE' '$CONSOLIDATED_PATH'" \
    "Upload consolidated file to $CONSOLIDATED_PATH"

# Remove original small files
{% for file in directory.small_files %}
execute_command "hdfs dfs -rm '{{ file.path }}'" \
    "Remove small file {{ file.path }}"
{% endfor %}

# Clean up temporary file
execute_command "rm -f '$TEMP_FILE'" \
    "Clean up temporary file"

log INFO "Consolidated {{ directory.small_files|length }} files in {{ directory.path }}"
{% endfor %}
log INFO "Small files consolidation completed"
{% endif %}

{% if optimization.category == "replication" %}
# ========================================
# Replication Optimization
# ========================================
log INFO "Starting replication optimization..."
log INFO "Files to process: {{ optimization.files|length }}"

{% for file in optimization.files %}
# Optimizing replication for: {{ file.path }}
log INFO "Reducing replication for {{ file.path }} from {{ file.current_replication }} to {{ file.suggested_replication }}"

execute_command "hdfs dfs -setrep {{ file.suggested_replication }} '{{ file.path }}'" \
    "Set replication factor for {{ file.path }}"

# Verify replication
if [ "$DRY_RUN" = "false" ]; then
    REPLICATION=$(hdfs dfs -stat %r '{{ file.path }}')
    log INFO "New replication factor for {{ file.path }}: $REPLICATION"
fi

{% endfor %}
log INFO "Replication optimization completed"
{% endif %}

{% if optimization.category == "cleanup" %}
# ========================================
# Cleanup Orphaned Files
# ========================================
log INFO "Starting cleanup of orphaned files..."
log INFO "Files to remove: {{ optimization.files|length }}"

{% for file in optimization.files %}
# Removing orphaned file: {{ file.path }}
log INFO "Removing orphaned file {{ file.path }} ({{ file.age_days|round(1) }} days old)"

# Create safety backup for critical files
{% if file.cleanup_priority == "critical" %}
execute_command "hdfs dfs -cp '{{ file.path }}' '$BACKUP_DIR/$(basename '{{ file.path }}')'" \
    "Backup critical file before deletion"
{% endif %}

# Remove the file (skip trash for temp files)
execute_command "hdfs dfs -rm -skipTrash '{{ file.path }}'" \
    "Remove orphaned file {{ file.path }}"

{% endfor %}
log INFO "Cleanup completed"
{% endif %}

{% if optimization.category == "compression" %}
# ========================================
# Data Compression
# ========================================
log INFO "Starting data compression optimization..."
log INFO "Files to compress: {{ optimization.files|length }}"

{% for file in optimization.files %}
# Compressing: {{ file.path }}
log INFO "Compressing {{ file.path }} ({{ file.size_gb|round(2) }}GB)"

TEMP_COMPRESSED="/tmp/compressed_$(basename '{{ file.path }}').gz"
COMPRESSED_PATH="{{ file.path }}.gz"

# Download, compress, and re-upload
execute_command "hdfs dfs -get '{{ file.path }}' - | gzip > '$TEMP_COMPRESSED'" \
    "Download and compress {{ file.path }}"

execute_command "hdfs dfs -put '$TEMP_COMPRESSED' '$COMPRESSED_PATH'" \
    "Upload compressed file"

execute_command "hdfs dfs -rm '{{ file.path }}'" \
    "Remove original uncompressed file"

execute_command "rm -f '$TEMP_COMPRESSED'" \
    "Clean up temporary compressed file"

log INFO "Compressed {{ file.path }} successfully"
{% endfor %}
log INFO "Data compression completed"
{% endif %}

{% endfor %}

# ========================================
# Post-Optimization Tasks
# ========================================
log INFO "Running post-optimization tasks..."

# Run HDFS balancer
log INFO "Starting HDFS balancer..."
execute_command "hdfs balancer -threshold 5" \
    "Run HDFS balancer"

# Generate post-optimization report
log INFO "Generating post-optimization report..."
hdfs dfsadmin -report > "$BACKUP_DIR/cluster_report_after.txt"
hdfs fsck / -files -blocks > "$BACKUP_DIR/fsck_after.txt"

# Calculate actual savings
log INFO "Calculating storage savings..."
BEFORE_USED=$(grep "DFS Used:" "$BACKUP_DIR/cluster_report_before.txt" | awk '{print $3}')
AFTER_USED=$(grep "DFS Used:" "$BACKUP_DIR/cluster_report_after.txt" | awk '{print $3}')

log INFO "Storage before optimization: $BEFORE_USED"
log INFO "Storage after optimization: $AFTER_USED"

# Generate summary
cat > "$BACKUP_DIR/optimization_summary.txt" << EOF
HDFS Cost Optimization Summary
Generated: $(date)
Optimization Plan ID: {{ plan_id }}

Estimated Savings:
- Monthly: ${{ total_monthly_savings }}
- Annual: ${{ total_annual_savings }}

Affected Data: {{ affected_data_gb }}GB

Optimization Categories:
{% for opt in optimizations %}
- {{ opt.category }}: {{ opt.files|length }} files
{% endfor %}

Backup Location: $BACKUP_DIR
Log File: $LOG_FILE
EOF

log INFO "Optimization completed successfully!"
log INFO "Summary saved to: $BACKUP_DIR/optimization_summary.txt"
log INFO "Backup directory: $BACKUP_DIR"

# Display final summary
echo
echo "========================================"
echo "HDFS COST OPTIMIZATION COMPLETED"
echo "========================================"
echo "Estimated Monthly Savings: ${{ total_monthly_savings }}"
echo "Estimated Annual Savings: ${{ total_annual_savings }}"
echo "Affected Data: {{ affected_data_gb }}GB"
echo "Backup Location: $BACKUP_DIR"
echo "Log File: $LOG_FILE"
echo "========================================"
""")
        
        return script_template.render(
            timestamp=datetime.utcnow().isoformat(),
            plan_id=optimization_plan.get("plan_id", "unknown"),
            optimizations=optimization_plan.get("optimizations", []),
            total_monthly_savings=optimization_plan.get("total_monthly_savings", 0),
            total_annual_savings=optimization_plan.get("total_annual_savings", 0),
            affected_data_gb=optimization_plan.get("affected_data_gb", 0)
        )
    
    def generate_monitoring_script(self) -> str:
        """Generate monitoring script for post-optimization tracking"""
        
        monitoring_template = Template("""#!/bin/bash
# HDFS Cost Monitoring Script
# Run this script regularly to track optimization effectiveness

LOG_FILE="/var/log/hdfs_monitoring_$(date +%Y%m%d).log"
REPORT_FILE="/var/log/hdfs_cost_report_$(date +%Y%m%d).json"

# Colors
GREEN='\\033[0;32m'
YELLOW='\\033[1;33m'
RED='\\033[0;31m'
NC='\\033[0m'

log() {
    local level=$1
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case $level in
        ERROR)
            echo -e "${RED}[$timestamp] ERROR: $message${NC}" | tee -a "$LOG_FILE"
            ;;
        WARN)
            echo -e "${YELLOW}[$timestamp] WARN: $message${NC}" | tee -a "$LOG_FILE"
            ;;
        INFO)
            echo -e "${GREEN}[$timestamp] INFO: $message${NC}" | tee -a "$LOG_FILE"
            ;;
    esac
}

# Function to get cluster metrics
get_cluster_metrics() {
    local report=$(hdfs dfsadmin -report)
    
    local total_capacity=$(echo "$report" | grep "Configured Capacity:" | awk '{print $3}')
    local used_capacity=$(echo "$report" | grep "DFS Used:" | awk '{print $3}')
    local remaining_capacity=$(echo "$report" | grep "DFS Remaining:" | awk '{print $3}')
    
    echo "{"
    echo "  \"cluster_metrics\": {"
    echo "    \"total_capacity\": \"$total_capacity\","
    echo "    \"used_capacity\": \"$used_capacity\","
    echo "    \"remaining_capacity\": \"$remaining_capacity\","
    echo "    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\""
    echo "  },"
}

# Function to analyze file distribution
analyze_file_distribution() {
    log INFO "Analyzing file distribution..."
    
    local total_files=$(hdfs dfs -count -h / | awk '{print $2}')
    local total_size=$(hdfs dfs -du -s -h / | awk '{print $1}')
    
    # Count small files (< 64MB)
    local small_files=$(hdfs dfs -find / -type f -exec hdfs dfs -stat %b {} \\; | awk '$1 < 67108864' | wc -l)
    
    echo "  \"file_distribution\": {"
    echo "    \"total_files\": $total_files,"
    echo "    \"total_size\": \"$total_size\","
    echo "    \"small_files_count\": $small_files,"
    echo "    \"small_files_percentage\": $(echo "scale=2; $small_files * 100 / $total_files" | bc -l)"
    echo "  },"
}

# Function to analyze storage policies
analyze_storage_policies() {
    log INFO "Analyzing storage policies..."
    
    local hot_files=$(hdfs dfs -find / -type f -exec hdfs storagepolicies -getStoragePolicy -path {} \\; | grep -c "HOT" || echo 0)
    local cold_files=$(hdfs dfs -find / -type f -exec hdfs storagepolicies -getStoragePolicy -path {} \\; | grep -c "COLD" || echo 0)
    local warm_files=$(hdfs dfs -find / -type f -exec hdfs storagepolicies -getStoragePolicy -path {} \\; | grep -c "WARM" || echo 0)
    
    echo "  \"storage_policies\": {"
    echo "    \"hot_files\": $hot_files,"
    echo "    \"cold_files\": $cold_files,"
    echo "    \"warm_files\": $warm_files"
    echo "  },"
}

# Function to analyze replication
analyze_replication() {
    log INFO "Analyzing replication factors..."
    
    local avg_replication=$(hdfs fsck / -files -blocks -replication | grep "Average block replication" | awk '{print $4}')
    local over_replicated=$(hdfs fsck / -files -blocks -replication | grep "Over-replicated" | wc -l)
    local under_replicated=$(hdfs fsck / -files -blocks -replication | grep "Under-replicated" | wc -l)
    
    echo "  \"replication_analysis\": {"
    echo "    \"average_replication\": \"$avg_replication\","
    echo "    \"over_replicated_blocks\": $over_replicated,"
    echo "    \"under_replicated_blocks\": $under_replicated"
    echo "  },"
}

# Function to calculate cost estimates
calculate_cost_estimates() {
    log INFO "Calculating cost estimates..."
    
    local total_gb=$(hdfs dfs -du -s / | awk '{print $1 / 1024 / 1024 / 1024}')
    local estimated_monthly_cost=$(echo "scale=2; $total_gb * 0.04 * 3" | bc -l)
    local estimated_annual_cost=$(echo "scale=2; $estimated_monthly_cost * 12" | bc -l)
    
    echo "  \"cost_estimates\": {"
    echo "    \"total_storage_gb\": $total_gb,"
    echo "    \"estimated_monthly_cost\": $estimated_monthly_cost,"
    echo "    \"estimated_annual_cost\": $estimated_annual_cost"
    echo "  }"
}

# Main monitoring execution
main() {
    log INFO "Starting HDFS cost monitoring..."
    
    # Generate JSON report
    {
        get_cluster_metrics
        analyze_file_distribution
        analyze_storage_policies
        analyze_replication
        calculate_cost_estimates
        echo "}"
    } > "$REPORT_FILE"
    
    log INFO "Monitoring report generated: $REPORT_FILE"
    
    # Display summary
    echo
    echo "========================================"
    echo "HDFS COST MONITORING SUMMARY"
    echo "========================================"
    cat "$REPORT_FILE" | python3 -m json.tool 2>/dev/null || cat "$REPORT_FILE"
    echo "========================================"
    echo "Report saved to: $REPORT_FILE"
    echo "Log file: $LOG_FILE"
    echo "========================================"
}

# Run main function
main "$@"
""")
        
        return monitoring_template.render()
    
    def generate_rollback_script(self, optimization_id: str) -> str:
        """Generate rollback script for optimization"""
        
        rollback_template = Template("""#!/bin/bash
# HDFS Optimization Rollback Script
# Generated: {{ timestamp }}
# Optimization ID: {{ optimization_id }}

set -e

LOG_FILE="/var/log/hdfs_rollback_$(date +%Y%m%d_%H%M%S).log"
BACKUP_DIR="/tmp/hdfs_backup_*"  # Find the most recent backup

# Colors
RED='\\033[0;31m'
GREEN='\\033[0;32m'
YELLOW='\\033[1;33m'
NC='\\033[0m'

log() {
    local level=$1
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case $level in
        ERROR)
            echo -e "${RED}[$timestamp] ERROR: $message${NC}" | tee -a "$LOG_FILE"
            ;;
        WARN)
            echo -e "${YELLOW}[$timestamp] WARN: $message${NC}" | tee -a "$LOG_FILE"
            ;;
        INFO)
            echo -e "${GREEN}[$timestamp] INFO: $message${NC}" | tee -a "$LOG_FILE"
            ;;
    esac
}

# Find most recent backup
find_backup_dir() {
    BACKUP_DIR=$(ls -dt /tmp/hdfs_backup_* 2>/dev/null | head -1)
    if [ -z "$BACKUP_DIR" ]; then
        log ERROR "No backup directory found"
        exit 1
    fi
    log INFO "Using backup directory: $BACKUP_DIR"
}

# Rollback function
rollback_optimization() {
    log INFO "Starting rollback for optimization {{ optimization_id }}"
    
    # This is a template - specific rollback actions would be generated
    # based on the original optimization plan
    
    log WARN "Rollback functionality requires manual implementation"
    log INFO "Please review the backup at: $BACKUP_DIR"
    log INFO "And manually reverse the optimization steps"
    
    # Example rollback actions:
    # - Restore files from backup
    # - Reset storage policies
    # - Adjust replication factors
    # - Restore deleted files
}

# Main execution
main() {
    log INFO "HDFS Optimization Rollback Starting..."
    
    find_backup_dir
    rollback_optimization
    
    log INFO "Rollback completed"
    log INFO "Log file: $LOG_FILE"
}

main "$@"
""")
        
        return rollback_template.render(
            timestamp=datetime.utcnow().isoformat(),
            optimization_id=optimization_id
        )
    
    def store_optimization_plan(self, plan_id: str, plan: Dict[str, Any]) -> None:
        """Store optimization plan for script generation"""
        self.script_storage[plan_id] = {
            "plan": plan,
            "created_at": datetime.utcnow().isoformat(),
            "status": "ready"
        }
        self.logger.info(f"Stored optimization plan: {plan_id}")
    
    def get_optimization_plan(self, plan_id: str) -> Dict[str, Any]:
        """Retrieve stored optimization plan"""
        if plan_id not in self.script_storage:
            raise ValueError(f"Optimization plan not found: {plan_id}")
        
        return self.script_storage[plan_id]["plan"]

# Module-level functions for use by server
script_generator = HDFSScriptGenerator()

def create_optimization_script(optimization_id: str) -> str:
    """Main function to generate optimization script"""
    try:
        optimization_plan = script_generator.get_optimization_plan(optimization_id)
        script = script_generator.generate_optimization_script(optimization_plan)
        return script
    except Exception as e:
        logging.error(f"Failed to generate script for {optimization_id}: {e}")
        return f"#!/bin/bash\necho 'Script generation failed: {str(e)}'\nexit 1"

def create_monitoring_script() -> str:
    """Generate monitoring script"""
    return script_generator.generate_monitoring_script()

def create_rollback_script(optimization_id: str) -> str:
    """Generate rollback script"""
    return script_generator.generate_rollback_script(optimization_id)

def store_optimization_plan(plan_id: str, plan: Dict[str, Any]) -> None:
    """Store optimization plan"""
    script_generator.store_optimization_plan(plan_id, plan)