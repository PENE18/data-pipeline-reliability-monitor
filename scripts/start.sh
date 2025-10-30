#!/bin/bash

echo "🚀 Starting Data Pipeline Services..."
echo ""

# Build images
echo "🔨 Building Docker images..."
docker-compose build

# Start services
echo "▶️  Starting services..."
docker-compose up -d

# Wait for services
echo ""
echo "⏳ Waiting for services to be healthy..."
sleep 10

# Check status
echo ""
echo "📊 Service Status:"
docker-compose ps

echo ""
echo "✅ Services started!"
echo ""
echo "Access:"
echo "  - Airflow:    http://localhost:8080 (airflow/airflow)"
echo "  - Grafana:    http://localhost:3000 (admin/admin)"
echo "  - Prometheus: http://localhost:9090"
echo ""
echo "View logs: docker-compose logs -f"