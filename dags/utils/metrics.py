"""
Prometheus Metrics Exporter
Exports pipeline metrics to Prometheus Pushgateway
"""

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from datetime import datetime
import traceback

# Pushgateway configuration
PUSHGATEWAY_URL = 'pushgateway:9091'


def export_metrics_to_prometheus(
    task_name,
    duration,
    record_count,
    status,
    quality_score=None,
    dag_id='unknown'
):
    """
    Export task metrics to Prometheus Pushgateway
    
    Args:
        task_name (str): Name of the task (e.g., 'extract', 'load', 'transform')
        duration (float): Task execution duration in seconds
        record_count (int): Number of records processed
        status (str): Task status ('success' or 'failure')
        quality_score (float, optional): Data quality score (0-100)
        dag_id (str): DAG identifier
    """
    try:
        # Create a new registry for this push
        registry = CollectorRegistry()
        
        # Metric 1: Task Duration
        duration_gauge = Gauge(
            'pipeline_task_duration_seconds',
            'Task execution duration in seconds',
            ['task_name', 'dag_id'],
            registry=registry
        )
        duration_gauge.labels(task_name=task_name, dag_id=dag_id).set(duration)
        
        # Metric 2: Records Processed
        records_gauge = Gauge(
            'pipeline_records_processed_total',
            'Total number of records processed',
            ['task_name', 'dag_id'],
            registry=registry
        )
        records_gauge.labels(task_name=task_name, dag_id=dag_id).set(record_count)
        
        # Metric 3: Task Status (0=success, 1=failure)
        status_gauge = Gauge(
            'pipeline_task_status',
            'Task execution status (0=success, 1=failure)',
            ['task_name', 'dag_id'],
            registry=registry
        )
        status_value = 0 if status == 'success' else 1
        status_gauge.labels(task_name=task_name, dag_id=dag_id).set(status_value)
        
        # Metric 4: Data Quality Score (if provided)
        if quality_score is not None:
            quality_gauge = Gauge(
                'pipeline_data_quality_score',
                'Data quality score percentage (0-100)',
                ['dag_id'],
                registry=registry
            )
            quality_gauge.labels(dag_id=dag_id).set(quality_score)
        
        # Metric 5: Last Execution Timestamp
        timestamp_gauge = Gauge(
            'pipeline_task_last_execution',
            'Unix timestamp of last task execution',
            ['task_name', 'dag_id'],
            registry=registry
        )
        timestamp_gauge.labels(task_name=task_name, dag_id=dag_id).set(
            datetime.now().timestamp()
        )
        
        # Push all metrics to Pushgateway
        push_to_gateway(
            PUSHGATEWAY_URL,
            job=f'airflow_task_{task_name}',
            registry=registry,
            timeout=5
        )
        
        print(f"📊 Metrics exported successfully: {task_name}")
        print(f"   Duration: {duration:.2f}s")
        print(f"   Records: {record_count}")
        print(f"   Status: {status}")
        if quality_score is not None:
            print(f"   Quality: {quality_score:.1f}%")
        
    except Exception as e:
        print(f"⚠️  Failed to export metrics to Prometheus: {e}")
        print(f"   Task: {task_name}")
        print(f"   Pushgateway: {PUSHGATEWAY_URL}")
        traceback.print_exc()


def export_pipeline_metrics(dag_id, status, total_duration, total_records):
    """
    Export overall pipeline metrics
    
    Args:
        dag_id (str): DAG identifier
        status (str): Overall pipeline status
        total_duration (float): Total pipeline duration
        total_records (int): Total records processed
    """
    try:
        registry = CollectorRegistry()
        
        # Overall pipeline duration
        pipeline_duration = Gauge(
            'pipeline_total_duration_seconds',
            'Total pipeline execution duration',
            ['dag_id'],
            registry=registry
        )
        pipeline_duration.labels(dag_id=dag_id).set(total_duration)
        
        # Overall pipeline status
        pipeline_status = Gauge(
            'pipeline_overall_status',
            'Overall pipeline status (0=success, 1=failure)',
            ['dag_id'],
            registry=registry
        )
        pipeline_status.labels(dag_id=dag_id).set(0 if status == 'success' else 1)
        
        # Total records processed
        pipeline_records = Gauge(
            'pipeline_total_records',
            'Total records processed by pipeline',
            ['dag_id'],
            registry=registry
        )
        pipeline_records.labels(dag_id=dag_id).set(total_records)
        
        push_to_gateway(
            PUSHGATEWAY_URL,
            job=f'airflow_pipeline_{dag_id}',
            registry=registry,
            timeout=5
        )
        
        print(f"📊 Pipeline metrics exported: {dag_id}")
        
    except Exception as e:
        print(f"⚠️  Failed to export pipeline metrics: {e}")


# Test function
if __name__ == "__main__":
    print("🧪 Testing metrics export...")
    
    export_metrics_to_prometheus(
        task_name='test_task',
        duration=10.5,
        record_count=100,
        status='success',
        quality_score=95.5,
        dag_id='test_dag'
    )
    
    print("✅ Test complete - check Pushgateway at http://localhost:9091")