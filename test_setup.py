#!/usr/bin/env python3
"""
Test script to verify HDFS Cost Advisor setup
"""

import sys
import os
import importlib.util

def test_python_version():
    """Test Python version compatibility"""
    print("🐍 Testing Python version...")
    if sys.version_info < (3, 11):
        print(f"❌ Python {sys.version} is too old. Need Python 3.11+")
        return False
    print(f"✅ Python {sys.version} is compatible")
    return True

def test_imports():
    """Test if all required packages can be imported"""
    print("\n📦 Testing package imports...")
    
    required_packages = [
        ("json", "json"),
        ("uuid", "uuid"),
        ("asyncio", "asyncio"),
        ("typing", "typing"),
        ("datetime", "datetime"),
        ("pydantic", "pydantic"),
        ("jinja2", "jinja2"),
        ("requests", "requests"),
    ]
    
    success = True
    for package_name, import_name in required_packages:
        try:
            __import__(import_name)
            print(f"✅ {package_name}")
        except ImportError as e:
            print(f"❌ {package_name}: {e}")
            success = False
    
    return success

def test_project_structure():
    """Test if project files exist"""
    print("\n📁 Testing project structure...")
    
    required_files = [
        "src/hdfs_cost_advisor/__init__.py",
        "src/hdfs_cost_advisor/server.py",
        "src/hdfs_cost_advisor/demo.py",
        "src/hdfs_cost_advisor/hdfs/__init__.py",
        "src/hdfs_cost_advisor/hdfs/client.py",
        "src/hdfs_cost_advisor/hdfs/analyzer.py",
        "src/hdfs_cost_advisor/llm/__init__.py",
        "src/hdfs_cost_advisor/llm/client.py",
        "src/hdfs_cost_advisor/cost/__init__.py",
        "src/hdfs_cost_advisor/cost/calculator.py",
        "src/hdfs_cost_advisor/utils/__init__.py",
        "src/hdfs_cost_advisor/utils/config.py",
        "requirements.txt",
        ".env.example"
    ]
    
    success = True
    for file_path in required_files:
        if os.path.exists(file_path):
            print(f"✅ {file_path}")
        else:
            print(f"❌ {file_path} (missing)")
            success = False
    
    return success

def test_module_loading():
    """Test if the main modules can be loaded"""
    print("\n🔧 Testing module loading...")
    
    # Add src to path
    sys.path.insert(0, "src")
    
    modules_to_test = [
        "hdfs_cost_advisor",
        "hdfs_cost_advisor.hdfs.analyzer",
        "hdfs_cost_advisor.cost.calculator",
        "hdfs_cost_advisor.utils.config",
    ]
    
    success = True
    for module_name in modules_to_test:
        try:
            importlib.import_module(module_name)
            print(f"✅ {module_name}")
        except Exception as e:
            print(f"❌ {module_name}: {e}")
            success = False
    
    return success

def test_demo_mode():
    """Test if demo mode can be initialized"""
    print("\n🎭 Testing demo mode initialization...")
    
    try:
        sys.path.insert(0, "src")
        from hdfs_cost_advisor.demo import DemoMCPServer
        
        # Try to initialize demo server
        server = DemoMCPServer()
        print("✅ Demo server initialized successfully")
        
        # Test demo HDFS client
        metrics = server.hdfs_client.get_cluster_metrics()
        if "filesystem" in metrics:
            print("✅ Demo HDFS client working")
        else:
            print("❌ Demo HDFS client returned invalid metrics")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ Demo mode failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 HDFS Cost Advisor Setup Test")
    print("=" * 50)
    
    tests = [
        test_python_version,
        test_imports,
        test_project_structure,
        test_module_loading,
        test_demo_mode
    ]
    
    results = []
    for test_func in tests:
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test_func.__name__} failed with error: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    print("📋 Test Summary")
    print("=" * 50)
    
    passed = sum(results)
    total = len(results)
    
    if passed == total:
        print(f"🎉 All tests passed! ({passed}/{total})")
        print("\n✅ Your setup is ready!")
        print("\n🚀 Next steps:")
        print("1. Set up your .env file with LLM API key")
        print("2. Run: python -m hdfs_cost_advisor.demo")
        return True
    else:
        print(f"❌ {total - passed} tests failed ({passed}/{total} passed)")
        print("\n🔧 Please fix the issues above before proceeding.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)