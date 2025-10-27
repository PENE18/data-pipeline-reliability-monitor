-- Initialize PostgreSQL Database
-- This script runs when postgres container first starts

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Create sample tables (optional)
CREATE TABLE IF NOT EXISTS processed_data (
    id SERIAL PRIMARY KEY,
    data JSONB,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);