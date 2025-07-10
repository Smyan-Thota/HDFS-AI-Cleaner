.PHONY: help install install-dev test test-unit test-integration lint format clean build docker-build docker-run docker-stop setup-env

# Default target
help:
	@echo "Available targets:"
	@echo "  install      - Install package and dependencies"
	@echo "  install-dev  - Install package with development dependencies"
	@echo "  test         - Run all tests"
	@echo "  test-unit    - Run unit tests only"
	@echo "  test-integration - Run integration tests only"
	@echo "  lint         - Run linting checks"
	@echo "  format       - Format code with black"
	@echo "  clean        - Clean build artifacts"
	@echo "  build        - Build package"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run Docker containers"
	@echo "  docker-stop  - Stop Docker containers"
	@echo "  setup-env    - Set up development environment"

# Installation targets
install:
	pip install -r requirements.txt
	pip install -e .

install-dev:
	pip install -r requirements.txt
	pip install -e .[dev]

# Testing targets
test:
	pytest tests/ -v

test-unit:
	pytest tests/ -v -m unit

test-integration:
	pytest tests/ -v -m integration

test-coverage:
	pytest tests/ --cov=src/hdfs_cost_advisor --cov-report=html --cov-report=term

# Code quality targets
lint:
	flake8 src/ tests/
	mypy src/

format:
	black src/ tests/
	isort src/ tests/

check-format:
	black --check src/ tests/
	isort --check-only src/ tests/

# Build targets
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python setup.py sdist bdist_wheel

publish: build
	twine upload dist/*

# Docker targets
docker-build:
	docker build -t hdfs-cost-advisor:latest .

docker-run:
	docker-compose up -d

docker-stop:
	docker-compose down

docker-logs:
	docker-compose logs -f hdfs-cost-advisor

docker-clean:
	docker-compose down -v
	docker system prune -f

# Development environment setup
setup-env:
	@echo "Setting up development environment..."
	@if [ ! -f .env ]; then cp .env.example .env; echo "Created .env file from .env.example"; fi
	@echo "Please edit .env file with your configuration"
	python -m venv venv
	@echo "Virtual environment created. Activate with: source venv/bin/activate"
	@echo "Then run: make install-dev"

# HDFS cluster management
hadoop-start:
	docker-compose up -d namenode datanode

hadoop-stop:
	docker-compose stop namenode datanode

hadoop-logs:
	docker-compose logs -f namenode datanode

# Utility targets
shell:
	python -c "from hdfs_cost_advisor.server import *; import IPython; IPython.embed()"

requirements-update:
	pip-compile requirements.in
	pip-compile requirements-dev.in

security-check:
	bandit -r src/
	safety check

pre-commit-install:
	pre-commit install

pre-commit-run:
	pre-commit run --all-files

# Documentation targets
docs-build:
	cd docs && make html

docs-serve:
	cd docs/_build/html && python -m http.server 8080

# Monitoring targets
prometheus-config:
	@echo "Generating Prometheus configuration..."
	@mkdir -p monitoring/prometheus
	@cat > monitoring/prometheus/prometheus.yml << 'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'hdfs-cost-advisor'
    static_configs:
      - targets: ['hdfs-cost-advisor:8000']
    metrics_path: /metrics
    scrape_interval: 30s

  - job_name: 'hdfs-namenode'
    static_configs:
      - targets: ['namenode:9870']
    metrics_path: /jmx
    scrape_interval: 60s
EOF

grafana-config:
	@echo "Generating Grafana configuration..."
	@mkdir -p monitoring/grafana/provisioning/{dashboards,datasources}
	@cat > monitoring/grafana/provisioning/datasources/prometheus.yml << 'EOF'
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
EOF

# CI/CD targets
ci-test:
	pytest tests/ -v --junitxml=test-results.xml --cov=src/hdfs_cost_advisor --cov-report=xml

ci-build:
	docker build -t hdfs-cost-advisor:$(shell git rev-parse --short HEAD) .
	docker tag hdfs-cost-advisor:$(shell git rev-parse --short HEAD) hdfs-cost-advisor:latest

ci-deploy:
	@echo "Deploying to staging environment..."
	# Add deployment commands here