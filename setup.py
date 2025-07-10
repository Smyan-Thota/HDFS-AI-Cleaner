from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="hdfs-cost-advisor",
    version="1.0.0",
    author="HDFS Cost Advisor Team",
    author_email="support@hdfs-cost-advisor.com",
    description="MCP server for HDFS cost optimization and analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hdfs-cost-advisor/hadoop-mcp",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Monitoring",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.11",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.5.0",
            "pre-commit>=3.4.0",
        ],
        "kerberos": [
            "pykerberos>=1.3.0",
            "requests-kerberos>=0.14.0",
        ],
        "monitoring": [
            "prometheus-client>=0.17.0",
            "grafana-api>=1.0.3",
        ],
    },
    entry_points={
        "console_scripts": [
            "hdfs-cost-advisor=hdfs_cost_advisor.server:main",
        ],
    },
    include_package_data=True,
    package_data={
        "hdfs_cost_advisor": ["templates/*.j2", "static/*"],
    },
    zip_safe=False,
    keywords="hdfs hadoop cost optimization mcp server claude",
    project_urls={
        "Bug Reports": "https://github.com/hdfs-cost-advisor/hadoop-mcp/issues",
        "Source": "https://github.com/hdfs-cost-advisor/hadoop-mcp",
        "Documentation": "https://hdfs-cost-advisor.readthedocs.io/",
    },
)