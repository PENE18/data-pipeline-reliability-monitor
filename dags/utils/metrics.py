from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from datetime import datetime

PUSHGATEWAY_URL = 'pushgateway:9091'

def export_metrics_to_prometheus(task_name, duration, record_count, status, quality_score=None):
    """Export metrics to Prometheus Pushgateway"""
    try:
        registry = CollectorRegistry()
        
        # Task duration
        duration_gauge = Gauge(
            'pipeline_task_duration_seconds',
            'Task execution duration',
            ['task_name'],
            registry=registry
        )
        duration_gauge.labels(task_name=task_name).set(duration)
        
        # Records processed
        records_gauge = Gauge(
            'pipeline_records_processed_total',
            'Total records processed',
            ['task_name'],
            registry=registry
        )
        records_gauge.labels(task_name=task_name).set(record_count)
        
        # Task status (0=success, 1=failure)
        status_gauge = Gauge(
            'pipeline_task_status',
            'Task status',
            ['task_name'],
            registry=registry
        )
        status_gauge.labels(task_name=task_name).set(0 if status == 'success' else 1)
        
        # Data quality score (if provided)
        if quality_score is not None:
            quality_gauge = Gauge(
                'pipeline_data_quality_score',
                'Data quality percentage',
                registry=registry
            )
            quality_gauge.set(quality_score)
        
        # Push to gateway
        push_to_gateway(
            PUSHGATEWAY_URL,
            job=f'airflow_task_{task_name}',
            registry=registry
        )
        
        print(f"📊 Metrics exported to Prometheus: {task_name}")
        
    except Exception as e:
        print(f"⚠️  Failed to export metrics: {e}")