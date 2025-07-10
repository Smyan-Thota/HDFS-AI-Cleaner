import json
import logging
import asyncio
import uuid
from typing import Dict, Any, List, Optional
import os
import sys

# Simple MCP server implementation
class MCPServer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        
        # Initialize components with proper error handling
        try:
            from .utils.config import get_settings
            self.settings = get_settings()
            
            from .hdfs.client import HDFSClient
            self.hdfs_client = HDFSClient(self.settings.hdfs)
            
            from .llm.client import LLMClient
            self.llm_client = LLMClient(self.settings.llm.provider, self.settings.llm.api_key)
            
            from .cost.calculator import CostCalculator
            self.cost_calculator = CostCalculator(self.settings.cost.storage_costs)
            
            self.logger.info("MCP Server initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize MCP server: {e}")
            raise

    def get_tools(self) -> List[Dict[str, Any]]:
        """Return list of available tools"""
        return [
            {
                "name": "scan_hdfs",
                "description": "Scan HDFS paths for cost optimization opportunities",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "paths": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of HDFS paths to scan"
                        },
                        "scan_depth": {
                            "type": "integer",
                            "default": 3,
                            "description": "Maximum depth for directory traversal"
                        }
                    },
                    "required": ["paths"]
                }
            },
            {
                "name": "optimize_costs",
                "description": "Analyze scan results and generate cost optimization recommendations",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "scan_id": {
                            "type": "string",
                            "description": "ID of the completed scan to analyze"
                        }
                    },
                    "required": ["scan_id"]
                }
            },
            {
                "name": "generate_script",
                "description": "Generate executable bash/HDFS CLI scripts for cost optimization",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "optimization_id": {
                            "type": "string",
                            "description": "ID of the optimization plan to generate script for"
                        }
                    },
                    "required": ["optimization_id"]
                }
            },
            {
                "name": "get_summary",
                "description": "Get comprehensive summary of scan results and potential savings",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "scan_id": {
                            "type": "string",
                            "description": "ID of the scan to summarize"
                        }
                    },
                    "required": ["scan_id"]
                }
            },
            {
                "name": "get_cluster_health",
                "description": "Get current HDFS cluster health and metrics",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
        ]

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute tool by name"""
        try:
            if name == "scan_hdfs":
                return await self.scan_hdfs(**arguments)
            elif name == "optimize_costs":
                return await self.optimize_costs(**arguments)
            elif name == "generate_script":
                return await self.generate_script(**arguments)
            elif name == "get_summary":
                return await self.get_summary(**arguments)
            elif name == "get_cluster_health":
                return await self.get_cluster_health()
            else:
                raise ValueError(f"Unknown tool: {name}")
        except Exception as e:
            self.logger.error(f"Tool {name} failed: {e}")
            return {"error": str(e), "status": "failed"}

    async def scan_hdfs(self, paths: List[str], scan_depth: int = 3) -> Dict[str, Any]:
        """Scan HDFS paths for cost optimization opportunities"""
        try:
            from .endpoints import scan
            scan_id = str(uuid.uuid4())
            self.logger.info(f"Starting HDFS scan {scan_id} for paths: {paths}")
            
            result = scan.execute_scan(self.hdfs_client, paths, scan_depth)
            result["scan_id"] = scan_id
            
            self.logger.info(f"Scan {scan_id} completed successfully")
            return result
            
        except Exception as e:
            self.logger.error(f"HDFS scan failed: {e}")
            return {
                "error": str(e),
                "status": "failed",
                "scan_id": scan_id if 'scan_id' in locals() else None
            }

    async def optimize_costs(self, scan_id: str) -> Dict[str, Any]:
        """Analyze scan results and generate cost optimization recommendations"""
        try:
            from .endpoints import optimize
            self.logger.info(f"Starting cost optimization analysis for scan {scan_id}")
            
            result = await optimize.generate_recommendations(
                scan_id, self.hdfs_client, self.llm_client, self.cost_calculator
            )
            
            self.logger.info(f"Cost optimization analysis completed for scan {scan_id}")
            return result
            
        except Exception as e:
            self.logger.error(f"Cost optimization failed for scan {scan_id}: {e}")
            return {
                "error": str(e),
                "status": "failed",
                "scan_id": scan_id
            }

    async def generate_script(self, optimization_id: str) -> str:
        """Generate executable bash/HDFS CLI scripts for cost optimization"""
        try:
            from .endpoints import generate_script
            self.logger.info(f"Generating optimization script for {optimization_id}")
            
            script = generate_script.create_optimization_script(optimization_id)
            
            self.logger.info(f"Script generated successfully for {optimization_id}")
            return script
            
        except Exception as e:
            self.logger.error(f"Script generation failed for {optimization_id}: {e}")
            return f"# Script generation failed: {str(e)}"

    async def get_summary(self, scan_id: str) -> Dict[str, Any]:
        """Get comprehensive summary of scan results and potential savings"""
        try:
            from .endpoints import summary
            self.logger.info(f"Generating summary for scan {scan_id}")
            
            result = summary.generate_summary(scan_id, self.cost_calculator)
            
            self.logger.info(f"Summary generated successfully for scan {scan_id}")
            return result
            
        except Exception as e:
            self.logger.error(f"Summary generation failed for scan {scan_id}: {e}")
            return {
                "error": str(e),
                "status": "failed",
                "scan_id": scan_id
            }

    async def get_cluster_health(self) -> Dict[str, Any]:
        """Get current HDFS cluster health and metrics"""
        try:
            self.logger.info("Retrieving cluster health metrics")
            
            metrics = self.hdfs_client.get_cluster_metrics()
            
            health_status = {
                "cluster_metrics": metrics,
                "status": "healthy",
                "timestamp": metrics.get("timestamp")
            }
            
            self.logger.info("Cluster health retrieved successfully")
            return health_status
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve cluster health: {e}")
            return {
                "error": str(e),
                "status": "failed"
            }

# Global server instance
server = MCPServer()

# Simple command-line interface for testing
async def test_cli():
    """Simple CLI for testing the server"""
    print("HDFS Cost Advisor MCP Server - Test CLI")
    print("Available commands:")
    print("1. scan [paths...] - Scan HDFS paths")
    print("2. optimize [scan_id] - Optimize costs")
    print("3. script [opt_id] - Generate script")
    print("4. summary [scan_id] - Get summary")
    print("5. health - Get cluster health")
    print("6. quit - Exit")
    
    while True:
        try:
            command = input("\n> ").strip().split()
            if not command:
                continue
                
            if command[0] == "quit":
                break
            elif command[0] == "scan":
                paths = command[1:] if len(command) > 1 else ["/"]
                result = await server.scan_hdfs(paths)
                print(json.dumps(result, indent=2))
            elif command[0] == "optimize" and len(command) > 1:
                result = await server.optimize_costs(command[1])
                print(json.dumps(result, indent=2))
            elif command[0] == "script" and len(command) > 1:
                result = await server.generate_script(command[1])
                print(result)
            elif command[0] == "summary" and len(command) > 1:
                result = await server.get_summary(command[1])
                print(json.dumps(result, indent=2))
            elif command[0] == "health":
                result = await server.get_cluster_health()
                print(json.dumps(result, indent=2))
            else:
                print("Invalid command. Type 'quit' to exit.")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_cli())