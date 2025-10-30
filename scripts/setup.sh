#!/bin/bash

echo "=============================================="
echo "🚀 Data Pipeline Reliability Monitor Setup"
echo "=============================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

print_success "Docker and Docker Compose are installed"

# Create necessary directories
echo ""
echo "📁 Creating directories..."
mkdir -p data/raw
mkdir -p data/processed
mkdir -p data/db
mkdir -p dags/utils
mkdir -p spark/jobs
mkdir -p spark/jars
mkdir -p prometheus
mkdir -p grafana/dashboards
mkdir -p grafana/provisioning/dashboards
mkdir -p grafana/provisioning/datasources
mkdir -p scripts

print_success "Directories created"

# Create .env file if it doesn't exist
echo ""
echo "📝 Checking .env file..."
if [ ! -f .env ]; then
    echo "Creating .env file..."
    cat > .env << EOF
# Airflow Configuration
AIRFLOW_UID=$(id -u)
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# Slack Webhook (optional - add your webhook URL here)
SLACK_WEBHOOK_URL=

# Optional: Uncomment and add your Slack webhook
# Get yours at: https://api.slack.com/messaging/webhooks
# SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
EOF
    print_success ".env file created"
else
    print_warning ".env file already exists - skipping"
    # Update AIRFLOW_UID if needed
    if grep -q "AIRFLOW_UID=" .env; then
        sed -i.bak "s/AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
        print_success "Updated AIRFLOW_UID in .env"
    fi
fi

# Set permissions
echo ""
echo "🔒 Setting permissions..."
chmod -R 755 data/ 2>/dev/null || true
chmod -R 755 dags/ 2>/dev/null || true
chmod -R 755 spark/ 2>/dev/null || true
chmod +x scripts/*.sh 2>/dev/null || true
print_success "Permissions set"

# Download PostgreSQL JDBC driver for Spark
echo ""
echo "📦 Downloading PostgreSQL JDBC driver for Spark..."
JDBC_JAR="spark/jars/postgresql-42.6.0.jar"
if [ ! -f "$JDBC_JAR" ]; then
    echo "Downloading JDBC driver..."
    curl -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
         -o "$JDBC_JAR" \
         --progress-bar \
         --connect-timeout 10 \
         --max-time 60
    
    if [ $? -eq 0 ]; then
        print_success "JDBC driver downloaded successfully"
    else
        print_error "Failed to download JDBC driver"
        echo "You can download it manually from:"
        echo "https://jdbc.postgresql.org/download/postgresql-42.6.0.jar"
        echo "Save it to: $JDBC_JAR"
    fi
else
    print_success "JDBC driver already exists"
fi

# Create __init__.py files for Python packages
echo ""
echo "🐍 Creating Python package files..."
touch dags/__init__.py
touch dags/utils/__init__.py
print_success "Python package files created"

# Verify all required files exist
echo ""
echo "🔍 Verifying project files..."
REQUIRED_FILES=(
    "docker-compose.yml"
    "airflow/Dockerfile"
    "spark/Dockerfile"
    "dags/clickstream_etl.py"
    "dags/utils/data_generator.py"
    "dags/utils/metrics.py"
    "spark/jobs/transform_clickstream.py"
    "scripts/init_db.sql"
    "prometheus/prometheus.yml"
    "prometheus/statsd_mapping.yml"
    "grafana/provisioning/datasources/datasource.yml"
    "grafana/provisioning/dashboards/dashboard.yml"
    "grafana/dashboards/pipeline_dashboard.json"
)

MISSING_FILES=()
for file in "${REQUIRED_FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✓ $file"
    else
        echo "  ✗ $file (MISSING)"
        MISSING_FILES+=("$file")
    fi
done

if [ ${#MISSING_FILES[@]} -ne 0 ]; then
    echo ""
    print_error "Missing required files:"
    for file in "${MISSING_FILES[@]}"; do
        echo "  - $file"
    done
    echo ""
    echo "Please create these files before continuing."
    exit 1
fi

print_success "All required files present"

# Summary
echo ""
echo "=============================================="
echo "✅ Setup Complete!"
echo "=============================================="
echo ""
echo "📋 Next Steps:"
echo ""
echo "1. (Optional) Configure Slack webhook in .env file"
echo "   Edit .env and add your SLACK_WEBHOOK_URL"
echo ""
echo "2. Build and start all services:"
echo "   docker-compose build"
echo "   docker-compose up -d"
echo ""
echo "3. Wait for services to start (2-3 minutes)"
echo "   docker-compose logs -f"
echo ""
echo "4. Access the services:"
echo "   - Airflow UI:    http://localhost:8080 (airflow/airflow)"
echo "   - Grafana:       http://localhost:3000 (admin/admin)"
echo "   - Prometheus:    http://localhost:9090"
echo "   - Spark Master:  http://localhost:8081"
echo ""
echo "5. Trigger the pipeline:"
echo "   - Go to Airflow UI"
echo "   - Find 'clickstream_etl_pipeline' DAG"
echo "   - Toggle it ON (unpause)"
echo "   - Click 'Trigger DAG'"
echo ""
echo "6. Monitor in Grafana:"
echo "   - Go to Grafana"
echo "   - Navigate to 'Data Pipeline' folder"
echo "   - Open 'Data Pipeline Reliability Dashboard'"
echo ""
echo "=============================================="
echo ""