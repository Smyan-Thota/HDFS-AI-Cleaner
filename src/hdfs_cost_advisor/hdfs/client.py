from hdfs import InsecureClient, Config
try:
    from hdfs.ext.kerberos import KerberosClient
except ImportError:
    KerberosClient = None
import requests
import json
import logging
from typing import Dict, List, Any, Optional, Iterator
from dataclasses import dataclass
from datetime import datetime

@dataclass
class HDFSConfig:
    host: str
    port: int = 9000
    user: str = "hadoop"
    auth_type: str = "simple"
    namenode_web_port: int = 9870

class HDFSClient:
    def __init__(self, config: HDFSConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize HDFS client
        if config.auth_type == "kerberos" and KerberosClient:
            self.client = KerberosClient(f"http://{config.host}:{config.namenode_web_port}")
        else:
            self.client = InsecureClient(
                f"http://{config.host}:{config.namenode_web_port}",
                user=config.user
            )
        
        # Initialize JMX client
        self.jmx_base_url = f"http://{config.host}:{config.namenode_web_port}/jmx"
    
    def get_jmx_metrics(self, query: Optional[str] = None) -> Dict[str, Any]:
        """Retrieve JMX metrics from NameNode"""
        try:
            url = self.jmx_base_url
            if query:
                url += f"?qry={query}"
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to retrieve JMX metrics: {e}")
            raise
    
    def get_cluster_metrics(self) -> Dict[str, Any]:
        """Get comprehensive cluster metrics"""
        try:
            fs_metrics = self.get_jmx_metrics("Hadoop:service=NameNode,name=FSNamesystem")
            rpc_metrics = self.get_jmx_metrics("Hadoop:service=NameNode,name=RpcActivity")
            
            # Extract key metrics
            fs_bean = fs_metrics.get("beans", [{}])[0] if fs_metrics.get("beans") else {}
            rpc_bean = rpc_metrics.get("beans", [{}])[0] if rpc_metrics.get("beans") else {}
            
            return {
                "filesystem": {
                    "capacity_total": fs_bean.get("CapacityTotal", 0),
                    "capacity_used": fs_bean.get("CapacityUsed", 0),
                    "capacity_remaining": fs_bean.get("CapacityRemaining", 0),
                    "files_total": fs_bean.get("FilesTotal", 0),
                    "blocks_total": fs_bean.get("BlocksTotal", 0),
                    "under_replicated_blocks": fs_bean.get("UnderReplicatedBlocks", 0),
                    "corrupt_blocks": fs_bean.get("CorruptBlocks", 0)
                },
                "rpc": {
                    "rpc_queue_time_avg": rpc_bean.get("RpcQueueTimeAvgTime", 0),
                    "rpc_processing_time_avg": rpc_bean.get("RpcProcessingTimeAvgTime", 0)
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Failed to retrieve cluster metrics: {e}")
            # Return basic metrics as fallback
            return {
                "filesystem": {},
                "rpc": {},
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            }
    
    def analyze_file_metadata(self, path: str) -> Dict[str, Any]:
        """Analyze file metadata for cost optimization"""
        try:
            status = self.client.status(path)
            
            # Get detailed file information
            file_info = {
                "path": path,
                "size": status.get("length", 0),
                "replication": status.get("replication", 1),
                "block_size": status.get("blockSize", 0),
                "access_time": status.get("accessTime", 0),
                "modification_time": status.get("modificationTime", 0),
                "owner": status.get("owner", ""),
                "group": status.get("group", ""),
                "permission": status.get("permission", "")
            }
            
            # Calculate metadata efficiency
            if file_info["size"] > 0:
                file_info["efficiency_score"] = min(file_info["size"] / (128 * 1024 * 1024), 1.0)
            else:
                file_info["efficiency_score"] = 0.0
            
            return file_info
        except Exception as e:
            self.logger.error(f"Failed to analyze metadata for {path}: {e}")
            return {}
    
    def scan_directory_batch(self, path: str, max_depth: int = 3) -> Iterator[List[Dict[str, Any]]]:
        """Scan directory structure in batches for efficiency"""
        results = []
        try:
            # Use client.walk to traverse directory structure
            for root, dirs, files in self.client.walk(path, depth=max_depth):
                for file in files:
                    file_path = f"{root.rstrip('/')}/{file}"
                    file_info = self.analyze_file_metadata(file_path)
                    if file_info:
                        results.append(file_info)
                        
                        # Process in batches to manage memory
                        if len(results) >= 1000:
                            yield results
                            results = []
            
            if results:
                yield results
        except Exception as e:
            self.logger.error(f"Failed to scan directory {path}: {e}")
            raise
    
    def get_file_blocks(self, path: str) -> List[Dict[str, Any]]:
        """Get block information for a file"""
        try:
            # Use the WebHDFS API to get file status with block information
            url = f"http://{self.config.host}:{self.config.namenode_web_port}/webhdfs/v1{path}"
            params = {
                "op": "GET_BLOCK_LOCATIONS",
                "user.name": self.config.user
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if "LocatedBlocks" in data:
                return data["LocatedBlocks"].get("locatedBlocks", [])
            return []
        except Exception as e:
            self.logger.error(f"Failed to get block information for {path}: {e}")
            return []
    
    def get_directory_size(self, path: str) -> Dict[str, Any]:
        """Get directory size and file count"""
        try:
            # Use content_summary for efficient directory analysis
            summary = self.client.content(path, strict=False)
            
            return {
                "path": path,
                "size": summary.get("length", 0),
                "file_count": summary.get("fileCount", 0),
                "directory_count": summary.get("directoryCount", 0),
                "space_consumed": summary.get("spaceConsumed", 0),
                "quota": summary.get("quota", -1),
                "space_quota": summary.get("spaceQuota", -1)
            }
        except Exception as e:
            self.logger.error(f"Failed to get directory size for {path}: {e}")
            return {
                "path": path,
                "error": str(e)
            }
    
    def check_path_exists(self, path: str) -> bool:
        """Check if a path exists in HDFS"""
        try:
            self.client.status(path)
            return True
        except Exception:
            return False
    
    def get_storage_policy(self, path: str) -> str:
        """Get storage policy for a path"""
        try:
            url = f"http://{self.config.host}:{self.config.namenode_web_port}/webhdfs/v1{path}"
            params = {
                "op": "GETSTORAGEPOLICY",
                "user.name": self.config.user
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data.get("BlockStoragePolicy", {}).get("name", "HOT")
        except Exception as e:
            self.logger.error(f"Failed to get storage policy for {path}: {e}")
            return "HOT"  # Default policy