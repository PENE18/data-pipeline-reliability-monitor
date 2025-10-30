#!/bin/bash

echo "🏥 Health Check - Data Pipeline Services"
echo "========================================"
echo ""

# Check if docker-compose is running
if ! docker-compose ps | grep -q "Up"; then
    echo "❌ No services are running"
    echo "Run: ./scripts/start.sh"
    exit 1
fi

# Function to check service health
check_service() {
    local service=$1
    local port=$2
    local path=$3
    
    if curl -f -s "http://localhost:$port$path" > /dev/null 2>&1; then
        echo "✅ $service is healthy (http://localhost:$port$path)"
    else
        echo "❌ $service is NOT responding (http://localhost:$port$path)"
    fi
}

# Check each service
check_service "Airflow" 8080 "/health"
check_service "Grafana" 3000 "/api/health"
check_service "Prometheus" 9090 "/-/healthy"
check_service "Pushgateway" 9091 "/-/healthy"
check_service "Spark" 8081 ""

echo ""
echo "📊 Container Status:"
docker-compose ps

echo ""
echo "💾 Database Status:"
docker-compose exec -T postgres pg_isready -U airflow 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✅ PostgreSQL is ready"
    
    # Check table counts
    echo ""
    echo "📈 Data Statistics:"
    docker-compose exec -T postgres psql -U airflow -d airflow -c \
        "SELECT 'raw_clickstream' as table, COUNT(*) as records FROM raw_clickstream
         UNION ALL
         SELECT 'processed_sessions', COUNT(*) FROM processed_sessions
         UNION ALL
         SELECT 'pipeline_metrics', COUNT(*) FROM pipeline_metrics;" 2>/dev/null
else
    echo "❌ PostgreSQL is NOT ready"
fi

echo ""
echo "========================================"