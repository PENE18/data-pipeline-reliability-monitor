#!/bin/bash

echo "🛑 Stopping Data Pipeline Services..."
docker-compose down

echo ""
echo "✅ Services stopped"
echo ""
echo "To remove all data (volumes): docker-compose down -v"