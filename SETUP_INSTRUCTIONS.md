# 🚀 HDFS Cost Advisor - Complete Setup Instructions

**Follow these exact steps to get the HDFS Cost Advisor running and test all functionality.**

## ✅ Prerequisites Check

Before starting, ensure you have:
- **Python 3.11+** installed
- **Terminal/Command Line** access
- **Internet connection** (for installing packages and API calls)

## 📋 Step-by-Step Setup

### Step 1: Navigate to Project Directory

```bash
cd /Users/smyan/Desktop/sandbox/hadoop-mcp
```

### Step 2: Create Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate
```

**Important**: You should see `(venv)` in your terminal prompt after activation.

### Step 3: Install Dependencies

```bash
# Upgrade pip first
pip install --upgrade pip

# Install all required packages
pip install -r requirements.txt
```

### Step 4: Verify Installation

```bash
# Run the setup test
python test_setup.py
```

**Expected output**: All 5 tests should pass ✅

### Step 5: Configure Environment

```bash
# Copy the example environment file
cp .env.example .env

# Edit the .env file with your API key
# Use any text editor (nano, vim, VS Code, etc.)
nano .env
```

**Edit `.env` file and set your API key:**
```env
# LLM Configuration (REQUIRED)
LLM_PROVIDER=anthropic
LLM_API_KEY=your-actual-api-key-here

# Demo settings (these work as-is)
HDFS_HOST=localhost
HDFS_PORT=9000
HDFS_USER=demo
LOG_LEVEL=INFO
```

### Step 6: Get Your LLM API Key

#### Option A: Anthropic Claude (Recommended)
1. Go to https://console.anthropic.com/
2. Sign up for an account
3. Navigate to "API Keys" 
4. Create a new API key
5. Copy the key (starts with `sk-ant-`)

#### Option B: OpenAI
1. Go to https://platform.openai.com/
2. Create account and go to "API Keys"
3. Create new key
4. Set `LLM_PROVIDER=openai` in `.env`

### Step 7: Test the Demo

```bash
# Run the demo mode
python -m hdfs_cost_advisor.demo
```

## 🎮 Demo Testing Workflow

Once the demo starts, test these commands **in order**:

### 1. Scan Demo Data
```bash
demo> scan /data /logs
```
**Expected**: Returns scan results with scan_id

### 2. Optimize Costs
```bash
demo> optimize <scan_id_from_step_1>
```
**Expected**: AI analysis with optimization recommendations

### 3. Generate Script
```bash
demo> script <optimization_id_from_step_2>
```
**Expected**: Bash script with HDFS optimization commands

### 4. Get Summary
```bash
demo> summary <scan_id_from_step_1>
```
**Expected**: Detailed cost analysis and savings projections

### 5. Check Health
```bash
demo> health
```
**Expected**: Cluster health metrics

### 6. Exit
```bash
demo> quit
```

## 🎯 Expected Demo Results

When everything works correctly, you should see:

```
=== HDFS Cost Advisor - DEMO MODE ===
This demo simulates HDFS cost analysis without requiring a real cluster

demo> scan /data
Scanning paths: ['/data']
{
  "scan_id": "abc123-def456-789",
  "status": "completed",
  "total_files": 58,
  "total_size_gb": 42.5,
  "demo_mode": true
}

demo> optimize abc123-def456-789
Optimizing scan: abc123-def456-789
Analysis: Based on the analysis of 58 files (42.5GB total), I've identified several significant cost optimization opportunities...
Recommendations: 4
Monthly savings: $340.80
Optimization ID: xyz789-uvw012-345
```

## 🔧 Troubleshooting

### Issue: "No module named 'pydantic'"
**Solution**: 
```bash
# Make sure virtual environment is activated
source venv/bin/activate
# Reinstall dependencies
pip install -r requirements.txt
```

### Issue: "LLM API error"
**Solution**:
1. Check your API key is correctly set in `.env`
2. Verify you have credits/quota with your LLM provider
3. Test with: `curl -H "x-api-key: YOUR_KEY" https://api.anthropic.com/v1/messages`

### Issue: "Module not found"
**Solution**:
```bash
# Set Python path
export PYTHONPATH=/Users/smyan/Desktop/sandbox/hadoop-mcp/src:$PYTHONPATH
# Or install in development mode
pip install -e .
```

### Issue: Demo doesn't start
**Solution**:
```bash
# Check Python version
python3 --version  # Should be 3.11+
# Run setup test
python test_setup.py
# Check virtual environment is activated
which python  # Should show venv path
```

## 🎉 Success Indicators

✅ **Setup test passes** - All 5 tests show green checkmarks  
✅ **Demo starts** - Shows the demo CLI prompt  
✅ **Scan works** - Returns file analysis with scan_id  
✅ **Optimization works** - AI generates recommendations  
✅ **Script generation works** - Creates bash scripts  
✅ **API calls work** - LLM responds within 10 seconds  

## 🏃‍♂️ Quick Start Alternative

If you prefer a one-command setup:

```bash
# Make the script executable and run it
chmod +x quick_start.sh
./quick_start.sh
```

This script will:
1. Check Python version
2. Create virtual environment
3. Install dependencies
4. Run tests
5. Set up .env file
6. Start demo (if API key is configured)

## 🔗 Integration with Claude Code

After successful testing, add to your Claude Code configuration:

```json
{
  "mcpServers": {
    "hdfs-cost-advisor": {
      "command": "python",
      "args": ["-m", "hdfs_cost_advisor.server"],
      "cwd": "/Users/smyan/Desktop/sandbox/hadoop-mcp",
      "env": {
        "PYTHONPATH": "/Users/smyan/Desktop/sandbox/hadoop-mcp/src"
      }
    }
  }
}
```

## 📞 Support

If you encounter issues:

1. **Run the setup test**: `python test_setup.py`
2. **Check the logs**: Look for error messages in terminal
3. **Verify API key**: Ensure it's valid and has quota
4. **Check Python version**: Must be 3.11+
5. **Verify virtual environment**: Should see `(venv)` in prompt

---

**🎯 Goal**: Get to a working demo where you can scan, optimize, and generate scripts!

**⏱️ Expected time**: 10-15 minutes for complete setup