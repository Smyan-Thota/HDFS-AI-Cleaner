# HDFS Cost Advisor MCP Server

A comprehensive HDFS cost optimization and analysis MCP server that integrates with Claude Code to provide intelligent cost analysis and automated optimization recommendations for large-scale HDFS deployments.

## Quick Start Guide

### Prerequisites

- **Python 3.11 or higher**
- **Git** (for cloning)
- **LLM API Key** (Anthropic Claude recommended)
- **Docker** (optional, for full HDFS setup)

### 🚀 Option 1: Demo Mode (Recommended for Testing)

**This is the easiest way to test the system without requiring a real HDFS cluster.**

#### Step 1: Set up the environment

```bash
# Navigate to the project directory
cd /Users/smyan/Desktop/sandbox/hadoop-mcp

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

#### Step 2: Configure environment variables

```bash
# Copy example environment file
cp .env.example .env

# Edit .env file with your LLM API key
# For demo mode, you only need to set the LLM configuration
```

**Edit `.env` file:**
```env
# LLM Configuration (required)
LLM_PROVIDER=anthropic
LLM_API_KEY=your-anthropic-api-key-here

# Demo mode settings (these work as-is)
HDFS_HOST=localhost
HDFS_PORT=9000
HDFS_USER=demo
LOG_LEVEL=INFO
```

#### Step 3: Run the demo

```bash
# Run the demo CLI
python -m hdfs_cost_advisor.demo
```

#### Step 4: Test the functionality

Once the demo starts, try these commands:

```bash
# 1. Scan demo HDFS data
demo> scan /data /logs

# 2. Optimize costs (use the scan_id from step 1)
demo> optimize <scan_id_from_step_1>

# 3. Generate optimization script (use optimization_id from step 2)
demo> script <optimization_id_from_step_2>

# 4. Get summary (use scan_id from step 1)
demo> summary <scan_id_from_step_1>

# 5. Check cluster health
demo> health

# 6. Exit
demo> quit
```

**Expected Demo Output:**
```
=== HDFS Cost Advisor - DEMO MODE ===
This demo simulates HDFS cost analysis without requiring a real cluster

Available commands:
1. scan [paths...] - Scan HDFS paths (demo data)
2. optimize [scan_id] - Generate optimization recommendations
3. script [opt_id] - Generate optimization script
4. summary [scan_id] - Get analysis summary
5. health - Get cluster health (demo)
6. quit - Exit

demo> scan /data
Scanning paths: ['/data']
{
  "scan_id": "abc123-def456",
  "status": "completed",
  "total_files": 58,
  "total_size_gb": 45.2,
  "demo_mode": true
}

Scan ID: abc123-def456
```

### 🐳 Option 2: Full Docker Setup (Complete HDFS Environment)

**This option provides a complete HDFS cluster for realistic testing.**

#### Step 1: Set up Docker environment

```bash
# Ensure Docker and Docker Compose are installed
docker --version
docker-compose --version

# Navigate to project directory
cd /Users/smyan/Desktop/sandbox/hadoop-mcp

# Copy and configure environment
cp .env.example .env
```

#### Step 2: Configure `.env` for Docker

```env
# HDFS Configuration
HDFS_HOST=namenode
HDFS_PORT=9000
HDFS_USER=hadoop
HDFS_NAMENODE_WEB_PORT=9870

# LLM Configuration
LLM_PROVIDER=anthropic
LLM_API_KEY=your-anthropic-api-key-here

# Docker settings
REDIS_ENABLED=true
REDIS_URL=redis://redis:6379
```

#### Step 3: Start the HDFS cluster

```bash
# Start all services (HDFS + Redis + Cost Advisor)
docker-compose up -d

# Check that services are running
docker-compose ps

# Wait for services to be ready (30-60 seconds)
docker-compose logs -f hdfs-cost-advisor
```

#### Step 4: Access the services

- **HDFS NameNode UI**: http://localhost:9870
- **Cost Advisor Logs**: `docker-compose logs hdfs-cost-advisor`
- **Redis**: Available at localhost:6379

#### Step 5: Test with real HDFS

```bash
# Enter the cost advisor container
docker-compose exec hdfs-cost-advisor bash

# Run the server CLI
python -m hdfs_cost_advisor.server

# Or test individual components
python -c "from hdfs_cost_advisor.server import server; print('Server loaded successfully')"
```

### 🧪 Option 3: Development Setup

**For developers who want to modify and extend the code.**

#### Step 1: Development environment

```bash
cd /Users/smyan/Desktop/sandbox/hadoop-mcp

# Install development dependencies
pip install -r requirements.txt
pip install pytest pytest-asyncio black flake8 mypy

# Install package in development mode
pip install -e .
```

#### Step 2: Run tests

```bash
# Run all tests
pytest

# Run specific test categories
pytest tests/test_server.py -v
pytest tests/test_hdfs_client.py -v

# Run with coverage
pytest --cov=src/hdfs_cost_advisor --cov-report=html
```

#### Step 3: Development workflow

```bash
# Format code
black src/ tests/

# Lint code
flake8 src/ tests/

# Type checking
mypy src/

# Use the Makefile for common tasks
make test
make lint
make format
```

## 🔧 Configuration Guide

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `LLM_PROVIDER` | LLM provider (anthropic/openai/google) | anthropic | Yes |
| `LLM_API_KEY` | API key for LLM service | - | Yes |
| `HDFS_HOST` | HDFS NameNode hostname | localhost | No |
| `HDFS_PORT` | HDFS port | 9000 | No |
| `HDFS_USER` | HDFS user | hadoop | No |
| `LOG_LEVEL` | Logging level | INFO | No |

### Getting LLM API Keys

#### Anthropic Claude (Recommended)
1. Go to https://console.anthropic.com/
2. Sign up for an account
3. Navigate to "API Keys"
4. Create a new API key
5. Copy the key to your `.env` file

#### OpenAI
1. Go to https://platform.openai.com/
2. Sign up and go to "API Keys"
3. Create new key
4. Set `LLM_PROVIDER=openai` in `.env`

#### Google Gemini
1. Go to https://ai.google.dev/
2. Get API key from Google AI Studio
3. Set `LLM_PROVIDER=google` in `.env`

## 📊 Example Usage Workflow

### Complete Demo Workflow

```bash
# 1. Start demo
python -m hdfs_cost_advisor.demo

# 2. Scan some paths
demo> scan /data /logs /tmp

# Expected output:
# - Finds ~50-60 files
# - Total size ~40-50GB
# - Identifies cold data, small files, etc.
# - Returns scan_id: abc123-def456

# 3. Generate optimization recommendations
demo> optimize abc123-def456

# Expected output:
# - AI analysis of optimization opportunities
# - Specific recommendations for cost savings
# - Monthly/annual savings estimates
# - Returns optimization_id: xyz789-uvw012

# 4. Generate executable script
demo> script xyz789-uvw012

# Expected output:
# - Complete bash script for implementing optimizations
# - HDFS commands for cold data migration
# - File consolidation procedures
# - Cleanup operations

# 5. Get detailed summary
demo> summary abc123-def456

# Expected output:
# - Comprehensive analysis report
# - Cost breakdown and projections
# - Risk assessment
# - Implementation timeline
```

### Real HDFS Example

```bash
# With real HDFS cluster running
python -m hdfs_cost_advisor.server

# Scan actual HDFS paths
> scan /user/data /warehouse /tmp

# This will:
# - Connect to real HDFS cluster
# - Analyze actual file metadata
# - Provide real cost optimization recommendations
```

## 🔍 Troubleshooting

### Common Issues

#### 1. "Module not found" errors
```bash
# Ensure Python path is set correctly
export PYTHONPATH=/Users/smyan/Desktop/sandbox/hadoop-mcp/src:$PYTHONPATH

# Or install in development mode
pip install -e .
```

#### 2. LLM API errors
```bash
# Check your API key is valid
echo $LLM_API_KEY

# Test API connection
curl -H "x-api-key: $LLM_API_KEY" https://api.anthropic.com/v1/messages
```

#### 3. HDFS connection errors (Docker mode)
```bash
# Check HDFS services are running
docker-compose ps

# Check NameNode is accessible
curl http://localhost:9870/

# View HDFS logs
docker-compose logs namenode
```

#### 4. Permission errors
```bash
# Ensure proper permissions
chmod +x src/hdfs_cost_advisor/server.py
```

### Debug Mode

Enable detailed logging:

```bash
# Set debug level
export LOG_LEVEL=DEBUG

# Run with verbose output
python -m hdfs_cost_advisor.demo
```

### Reset Demo Data

```bash
# The demo generates fresh mock data each time
# No cleanup needed - just restart the demo
```

## 🎯 What to Expect

### Demo Mode Results

- **Scan**: Analyzes ~50-60 mock files, ~40-50GB total
- **Analysis Time**: 2-3 seconds (simulated LLM call)
- **Recommendations**: 3-4 optimization strategies
- **Potential Savings**: 30-50% cost reduction
- **Script Generation**: Complete bash scripts with HDFS commands

### Performance Expectations

- **File Processing**: 1,000+ files per second (demo)
- **LLM Analysis**: 5-10 seconds (real API)
- **Script Generation**: < 1 second
- **Memory Usage**: < 100MB for demo mode

## 🔗 Integration with Claude Code

To use with Claude Code, add to your `claude.json`:

```json
{
  "mcpServers": {
    "hdfs-cost-advisor": {
      "command": "python",
      "args": ["-m", "hdfs_cost_advisor.server"],
      "cwd": "/Users/smyan/Desktop/sandbox/hadoop-mcp",
      "env": {
        "PYTHONPATH": "/Users/smyan/Desktop/sandbox/hadoop-mcp/src",
        "LLM_API_KEY": "your-api-key",
        "LOG_LEVEL": "INFO"
      }
    }
  }
}
```

## 📈 Next Steps

1. **Start with Demo Mode** to understand the functionality
2. **Try Docker Setup** for realistic HDFS testing
3. **Integrate with Claude Code** for AI-powered analysis
4. **Customize Cost Models** for your specific environment
5. **Deploy to Production** with real HDFS clusters

## 🆘 Support

If you encounter issues:

1. Check the troubleshooting section above
2. Enable debug logging: `export LOG_LEVEL=DEBUG`
3. Review the demo output for expected behavior
4. Test individual components in isolation

## ✅ Success Indicators

You'll know it's working when:

- ✅ Demo mode runs without errors
- ✅ Scan returns file analysis data
- ✅ LLM generates optimization recommendations
- ✅ Scripts are generated with HDFS commands
- ✅ Cost savings are calculated and displayed

**Ready to optimize your HDFS costs!** 🚀