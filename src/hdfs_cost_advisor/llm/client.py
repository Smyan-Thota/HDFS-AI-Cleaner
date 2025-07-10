import aiohttp
import asyncio
import json
from typing import Dict, Any, Optional
from enum import Enum
import logging

class LLMProvider(Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GOOGLE = "google"

class LLMClient:
    def __init__(self, provider: LLMProvider, api_key: str):
        self.provider = provider
        self.api_key = api_key
        self.logger = logging.getLogger(__name__)
        
        self.base_urls = {
            LLMProvider.OPENAI: "https://api.openai.com/v1",
            LLMProvider.ANTHROPIC: "https://api.anthropic.com/v1",
            LLMProvider.GOOGLE: "https://generativelanguage.googleapis.com/v1"
        }
    
    async def analyze_hdfs_cost_optimization(self, scan_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze HDFS scan results for cost optimization opportunities"""
        
        prompt = self._generate_cost_analysis_prompt(scan_results)
        
        try:
            async with aiohttp.ClientSession() as session:
                response = await self._make_llm_request(session, prompt)
                
                # Parse LLM response
                analysis = self._parse_cost_analysis(response)
                
                return analysis
                
        except Exception as e:
            self.logger.error(f"LLM analysis failed: {e}")
            # Return fallback analysis
            return self._generate_fallback_analysis(scan_results)
    
    def _generate_cost_analysis_prompt(self, scan_results: Dict[str, Any]) -> str:
        """Generate comprehensive cost analysis prompt"""
        total_files = scan_results.get('total_files', 0)
        total_size_gb = scan_results.get('total_size_gb', 0)
        cold_data_count = len(scan_results.get('cold_data', []))
        duplicate_count = len(scan_results.get('duplicate_candidates', []))
        small_files_count = len(scan_results.get('small_files', []))
        orphaned_count = len(scan_results.get('orphaned_files', []))
        
        # Calculate cold data size
        cold_data_size_gb = sum(
            file.get('size', 0) for file in scan_results.get('cold_data', [])
        ) / (1024 ** 3)
        
        # Calculate small files size
        small_files_size_gb = sum(
            file.get('size', 0) for file in scan_results.get('small_files', [])
        ) / (1024 ** 3)
        
        return f"""
You are an expert HDFS cost optimization analyst. Analyze the following HDFS scan data and provide detailed cost optimization recommendations.

HDFS Scan Results:
- Total files: {total_files}
- Total size: {total_size_gb:.2f} GB
- Cold data files: {cold_data_count} ({cold_data_size_gb:.2f} GB)
- Duplicate candidates: {duplicate_count}
- Small files: {small_files_count} ({small_files_size_gb:.2f} GB)
- Orphaned temp files: {orphaned_count}

Current Storage Patterns:
- Small files causing metadata overhead: {small_files_count} files
- Over-replicated data consuming extra storage
- Cold data on expensive storage tiers: {cold_data_size_gb:.2f} GB
- Orphaned temporary files wasting space: {orphaned_count} files

Provide analysis in the following JSON format:
{{
  "analysis_summary": "High-level analysis of optimization opportunities",
  "recommendations": [
    {{
      "title": "Optimization recommendation title",
      "description": "Detailed description of the optimization",
      "category": "cold_data|small_files|replication|cleanup",
      "impact": "high|medium|low",
      "estimated_savings_percent": 30,
      "estimated_savings_gb": 150.5,
      "implementation_complexity": "low|medium|high",
      "timeline": "immediate|1-2 weeks|1 month",
      "steps": ["Step 1", "Step 2", "Step 3"]
    }}
  ],
  "cost_calculations": {{
    "current_monthly_cost": 1500,
    "optimized_monthly_cost": 900,
    "monthly_savings": 600,
    "annual_savings": 7200
  }},
  "risk_assessment": {{
    "data_loss_risk": "low|medium|high",
    "performance_impact": "positive|neutral|negative",
    "downtime_required": "none|minimal|significant"
  }},
  "monitoring_recommendations": ["Metric 1", "Metric 2"],
  "confidence_score": 0.85
}}

Focus on practical, actionable recommendations with quantified cost savings.
"""
    
    async def _make_llm_request(self, session: aiohttp.ClientSession, prompt: str) -> str:
        """Make API request to LLM provider"""
        if self.provider == LLMProvider.OPENAI:
            return await self._openai_request(session, prompt)
        elif self.provider == LLMProvider.ANTHROPIC:
            return await self._anthropic_request(session, prompt)
        elif self.provider == LLMProvider.GOOGLE:
            return await self._google_request(session, prompt)
        else:
            raise ValueError(f"Unsupported LLM provider: {self.provider}")
    
    async def _openai_request(self, session: aiohttp.ClientSession, prompt: str) -> str:
        """Make request to OpenAI API"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": "gpt-4",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 3000,
            "temperature": 0.3
        }
        
        async with session.post(
            f"{self.base_urls[LLMProvider.OPENAI]}/chat/completions",
            headers=headers,
            json=data
        ) as response:
            if response.status != 200:
                raise Exception(f"OpenAI API error: {response.status}")
            
            result = await response.json()
            return result["choices"][0]["message"]["content"]
    
    async def _anthropic_request(self, session: aiohttp.ClientSession, prompt: str) -> str:
        """Make request to Anthropic API"""
        headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01"
        }
        
        data = {
            "model": "claude-3-sonnet-20240229",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 3000
        }
        
        async with session.post(
            f"{self.base_urls[LLMProvider.ANTHROPIC]}/messages",
            headers=headers,
            json=data
        ) as response:
            if response.status != 200:
                raise Exception(f"Anthropic API error: {response.status}")
            
            result = await response.json()
            return result["content"][0]["text"]
    
    async def _google_request(self, session: aiohttp.ClientSession, prompt: str) -> str:
        """Make request to Google Gemini API"""
        headers = {
            "Content-Type": "application/json"
        }
        
        data = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": 3000,
                "temperature": 0.3
            }
        }
        
        async with session.post(
            f"{self.base_urls[LLMProvider.GOOGLE]}/models/gemini-pro:generateContent?key={self.api_key}",
            headers=headers,
            json=data
        ) as response:
            if response.status != 200:
                raise Exception(f"Google API error: {response.status}")
            
            result = await response.json()
            return result["candidates"][0]["content"]["parts"][0]["text"]
    
    def _parse_cost_analysis(self, llm_response: str) -> Dict[str, Any]:
        """Parse LLM response into structured analysis"""
        try:
            # Extract JSON from LLM response
            start = llm_response.find("{")
            end = llm_response.rfind("}") + 1
            if start == -1 or end == 0:
                raise ValueError("No JSON found in response")
            
            json_str = llm_response[start:end]
            analysis = json.loads(json_str)
            
            # Validate required fields
            required_fields = ["analysis_summary", "recommendations", "cost_calculations"]
            for field in required_fields:
                if field not in analysis:
                    raise ValueError(f"Missing required field: {field}")
            
            # Validate recommendations structure
            for rec in analysis.get("recommendations", []):
                required_rec_fields = ["title", "description", "category", "impact"]
                for field in required_rec_fields:
                    if field not in rec:
                        self.logger.warning(f"Missing field {field} in recommendation")
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Failed to parse LLM response: {e}")
            raise ValueError(f"Invalid LLM response format: {e}")
    
    def _generate_fallback_analysis(self, scan_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate fallback analysis when LLM fails"""
        self.logger.warning("Using fallback analysis due to LLM failure")
        
        total_size_gb = scan_results.get('total_size_gb', 0)
        cold_data_count = len(scan_results.get('cold_data', []))
        small_files_count = len(scan_results.get('small_files', []))
        orphaned_count = len(scan_results.get('orphaned_files', []))
        
        # Calculate basic savings estimates
        cold_data_savings = sum(
            file.get('size', 0) for file in scan_results.get('cold_data', [])
        ) / (1024 ** 3) * 0.5  # 50% savings from cold storage
        
        current_cost = total_size_gb * 0.04 * 3  # $0.04/GB/month with 3x replication
        optimized_cost = current_cost - (cold_data_savings * 0.04 * 2)  # Savings from cold storage
        
        recommendations = []
        
        if cold_data_count > 0:
            recommendations.append({
                "title": "Cold Data Migration",
                "description": f"Move {cold_data_count} cold data files to cheaper storage tier",
                "category": "cold_data",
                "impact": "high",
                "estimated_savings_percent": 50,
                "estimated_savings_gb": cold_data_savings,
                "implementation_complexity": "medium",
                "timeline": "1-2 weeks",
                "steps": [
                    "Identify cold data files",
                    "Set cold storage policy",
                    "Migrate files to cold tier",
                    "Monitor storage costs"
                ]
            })
        
        if small_files_count > 0:
            recommendations.append({
                "title": "Small File Consolidation",
                "description": f"Consolidate {small_files_count} small files to reduce metadata overhead",
                "category": "small_files",
                "impact": "medium",
                "estimated_savings_percent": 20,
                "estimated_savings_gb": small_files_count * 0.001,
                "implementation_complexity": "high",
                "timeline": "1 month",
                "steps": [
                    "Identify small file directories",
                    "Create consolidation plan",
                    "Merge small files",
                    "Update processing scripts"
                ]
            })
        
        if orphaned_count > 0:
            recommendations.append({
                "title": "Orphaned File Cleanup",
                "description": f"Remove {orphaned_count} orphaned temporary files",
                "category": "cleanup",
                "impact": "low",
                "estimated_savings_percent": 10,
                "estimated_savings_gb": sum(
                    file.get('size', 0) for file in scan_results.get('orphaned_files', [])
                ) / (1024 ** 3),
                "implementation_complexity": "low",
                "timeline": "immediate",
                "steps": [
                    "Verify files are safe to delete",
                    "Create cleanup script",
                    "Execute cleanup",
                    "Monitor for issues"
                ]
            })
        
        return {
            "analysis_summary": "Automated analysis identified several optimization opportunities based on scan results.",
            "recommendations": recommendations,
            "cost_calculations": {
                "current_monthly_cost": current_cost,
                "optimized_monthly_cost": optimized_cost,
                "monthly_savings": current_cost - optimized_cost,
                "annual_savings": (current_cost - optimized_cost) * 12
            },
            "risk_assessment": {
                "data_loss_risk": "low",
                "performance_impact": "positive",
                "downtime_required": "minimal"
            },
            "monitoring_recommendations": [
                "Storage utilization metrics",
                "File access patterns",
                "Storage cost tracking"
            ],
            "confidence_score": 0.7,
            "source": "fallback_analysis"
        }