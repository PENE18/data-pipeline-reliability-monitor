#!/bin/bash

echo "⚠️  WARNING: This will remove ALL data and volumes!"
echo ""
read -p "Are you sure? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Reset cancelled"
    exit 0
fi

echo ""
echo "🗑️  Stopping services and removing volumes..."
docker-compose down -v

echo "🗑️  Removing local data..."
rm -rf data/raw/*
rm -rf data/processed/*

echo ""
echo "✅ Reset complete!"
echo ""
echo "Run './scripts/start.sh' to start fresh"