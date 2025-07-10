#!/bin/bash

# HDFS Cost Advisor Quick Start Script
# This script sets up the environment and runs the demo

set -e  # Exit on error

echo "🚀 HDFS Cost Advisor Quick Start"
echo "================================"

# Check if Python 3.11+ is available
echo "📋 Checking Python version..."
python3 --version

if ! python3 -c "import sys; exit(0 if sys.version_info >= (3, 11) else 1)" 2>/dev/null; then
    echo "❌ Python 3.11+ is required"
    exit 1
fi
echo "✅ Python version OK"

# Check if we're in the right directory
if [ ! -f "requirements.txt" ]; then
    echo "❌ Please run this script from the hadoop-mcp directory"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "📥 Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Run setup test
echo "🧪 Running setup test..."
python test_setup.py

if [ $? -ne 0 ]; then
    echo "❌ Setup test failed"
    exit 1
fi

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "⚙️  Creating .env file from example..."
    cp .env.example .env
    echo ""
    echo "🔑 Please edit .env file and add your LLM API key:"
    echo "   LLM_API_KEY=your-api-key-here"
    echo ""
    echo "📖 Get API keys from:"
    echo "   - Anthropic: https://console.anthropic.com/"
    echo "   - OpenAI: https://platform.openai.com/"
    echo ""
    echo "▶️  After setting up your API key, run the demo:"
    echo "   python -m hdfs_cost_advisor.demo"
else
    echo "✅ .env file exists"
    
    # Check if API key is set
    if grep -q "your-api-key-here" .env 2>/dev/null; then
        echo ""
        echo "🔑 Please update your LLM API key in .env file"
        echo "   Currently set to placeholder value"
        echo ""
        echo "▶️  After updating your API key, run the demo:"
        echo "   python -m hdfs_cost_advisor.demo"
    else
        echo ""
        echo "🎉 Setup complete! Starting demo..."
        echo ""
        # Run the demo
        python -m hdfs_cost_advisor.demo
    fi
fi