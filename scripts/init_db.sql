-- Create tables for clickstream data

-- Raw clickstream events
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

CREATE INDEX idx_raw_session ON raw_clickstream(session_id);
CREATE INDEX idx_raw_user ON raw_clickstream(user_id);
CREATE INDEX idx_raw_timestamp ON raw_clickstream(timestamp);

-- Processed session data
CREATE TABLE IF NOT EXISTS processed_sessions (
    session_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    event_count INTEGER NOT NULL,
    total_duration INTEGER NOT NULL,
    session_start TIMESTAMP NOT NULL,
    session_end TIMESTAMP NOT NULL,
    session_duration_minutes DOUBLE PRECISION,
    pages_visited TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_processed_user ON processed_sessions(user_id);
CREATE INDEX idx_processed_start ON processed_sessions(session_start);

-- Pipeline metrics
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    execution_date TIMESTAMP NOT NULL,
    task_name VARCHAR(100) NOT NULL,
    duration_seconds DOUBLE PRECISION NOT NULL,
    record_count INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    quality_score DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_metrics_date ON pipeline_metrics(execution_date);
CREATE INDEX idx_metrics_task ON pipeline_metrics(task_name);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;