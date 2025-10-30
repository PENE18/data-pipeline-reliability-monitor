-- ================================================
-- Data Pipeline Database Schema
-- ================================================

-- Table 1: Raw Clickstream Events
CREATE TABLE IF NOT EXISTS raw_clickstream (
    event_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    page_url VARCHAR(500) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    duration_seconds INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_raw_session ON raw_clickstream(session_id);
CREATE INDEX IF NOT EXISTS idx_raw_user ON raw_clickstream(user_id);
CREATE INDEX IF NOT EXISTS idx_raw_timestamp ON raw_clickstream(timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_created ON raw_clickstream(created_at);

-- Table 2: Processed Session Aggregations
CREATE TABLE IF NOT EXISTS processed_sessions (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    event_count INTEGER NOT NULL,
    total_duration INTEGER NOT NULL,
    session_start TIMESTAMP NOT NULL,
    session_end TIMESTAMP NOT NULL,
    session_duration_minutes DOUBLE PRECISION,
    pages_visited TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(session_id, created_at)
);

-- Indexes for analytics queries
CREATE INDEX IF NOT EXISTS idx_processed_user ON processed_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_processed_start ON processed_sessions(session_start);
CREATE INDEX IF NOT EXISTS idx_processed_session ON processed_sessions(session_id);

-- Table 3: Pipeline Metrics (for historical tracking)
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    execution_date TIMESTAMP NOT NULL,
    dag_id VARCHAR(100) NOT NULL,
    task_name VARCHAR(100) NOT NULL,
    duration_seconds DOUBLE PRECISION NOT NULL,
    record_count INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    quality_score DOUBLE PRECISION,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metrics_date ON pipeline_metrics(execution_date);
CREATE INDEX IF NOT EXISTS idx_metrics_task ON pipeline_metrics(task_name);
CREATE INDEX IF NOT EXISTS idx_metrics_status ON pipeline_metrics(status);

-- Grant all permissions to airflow user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;

-- Insert initial test data (optional)
COMMENT ON TABLE raw_clickstream IS 'Stores raw clickstream events from extraction';
COMMENT ON TABLE processed_sessions IS 'Stores aggregated session data from Spark transformation';
COMMENT ON TABLE pipeline_metrics IS 'Tracks pipeline execution metrics over time';