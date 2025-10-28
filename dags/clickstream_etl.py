from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
import json
import subprocess
import requests
import os
from utils.data_generator import generate_clickstream_data
from utils.metrics import export_metrics_to_prometheus

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=30),
    'sla': timedelta(minutes=30),
}

SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK_URL', '')

def send_slack_alert(message, context=None):
    """Send alert to Slack webhook"""
    if not SLACK_WEBHOOK:
        print("⚠️  Slack webhook not configured - skipping alert")
        return
    
    payload = {
        "text": f"🚨 *Pipeline Alert*\n{message}",
        "username": "Pipeline Monitor",
        "icon_emoji": ":rotating_light:"
    }
    
    if context:
        task_info = f"\n*Task:* {context.get('task_instance').task_id}\n*DAG:* {context.get('dag').dag_id}"
        payload["text"] += task_info
    
    try:
        requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
    except Exception as e:
        print(f"Error sending Slack alert: {e}")

with DAG(
    dag_id='clickstream_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline with monitoring and self-healing',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'clickstream', 'monitoring'],
    on_failure_callback=lambda ctx: send_slack_alert("❌ Pipeline FAILED!", ctx),
) as dag:

    @task()
    def extract_clickstream_data(**kwargs):
        """Extract: Generate clickstream data"""
        start_time = datetime.now()
        
        try:
            print("📥 Generating clickstream data...")
            data = generate_clickstream_data(num_events=1000)
            
            execution_date = kwargs['execution_date'].strftime('%Y%m%d_%H%M%S')
            raw_file = f'/opt/airflow/data/raw/clickstream_{execution_date}.json'
            
            with open(raw_file, 'w') as f:
                json.dump(data, f)
            
            duration = (datetime.now() - start_time).total_seconds()
            record_count = len(data)
            
            export_metrics_to_prometheus(
                task_name='extract',
                duration=duration,
                record_count=record_count,
                status='success'
            )
            
            print(f"✅ Extracted {record_count} records in {duration:.2f}s")
            return {'file_path': raw_file, 'record_count': record_count}
            
        except Exception as e:
            export_metrics_to_prometheus(
                task_name='extract',
                duration=(datetime.now() - start_time).total_seconds(),
                record_count=0,
                status='failure'
            )
            raise

    @task()
    def load_to_postgres(extract_result: dict, **kwargs):
        """Load: Store raw data in PostgreSQL"""
        start_time = datetime.now()
        file_path = extract_result['file_path']
        
        try:
            print(f"📤 Loading to PostgreSQL...")
            
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            pg_hook = PostgresHook(postgres_conn_id='postgres_default')
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            
            insert_query = """
                INSERT INTO raw_clickstream 
                (event_id, user_id, session_id, event_type, page_url, timestamp, duration_seconds)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
            """
            
            loaded_count = 0
            for event in data:
                cursor.execute(insert_query, (
                    event['event_id'],
                    event['user_id'],
                    event['session_id'],
                    event['event_type'],
                    event['page_url'],
                    event['timestamp'],
                    event.get('duration_seconds', 0)
                ))
                loaded_count += 1
            
            conn.commit()
            cursor.close()
            conn.close()
            
            duration = (datetime.now() - start_time).total_seconds()
            
            export_metrics_to_prometheus(
                task_name='load',
                duration=duration,
                record_count=loaded_count,
                status='success'
            )
            
            print(f"✅ Loaded {loaded_count} records in {duration:.2f}s")
            return {'loaded_count': loaded_count}
            
        except Exception as e:
            export_metrics_to_prometheus(
                task_name='load',
                duration=(datetime.now() - start_time).total_seconds(),
                record_count=0,
                status='failure'
            )
            raise

    @task()
    def transform_with_spark(load_result: dict, **kwargs):
        """Transform: Process with PySpark"""
        start_time = datetime.now()
        
        try:
            print("⚙️  Starting Spark transformation...")
            
            spark_job = '/opt/airflow/spark_jobs/transform_clickstream.py'
            execution_date = kwargs['execution_date'].strftime('%Y%m%d_%H%M%S')
            
            result = subprocess.run(
                ['python', spark_job, execution_date],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode != 0:
                raise Exception(f"Spark failed: {result.stderr}")
            
            print(result.stdout)
            
            processed_count = 0
            for line in result.stdout.split('\n'):
                if 'Processed sessions:' in line:
                    processed_count = int(line.split(':')[1].strip())
            
            duration = (datetime.now() - start_time).total_seconds()
            
            export_metrics_to_prometheus(
                task_name='transform',
                duration=duration,
                record_count=processed_count,
                status='success'
            )
            
            print(f"✅ Transformed {processed_count} sessions in {duration:.2f}s")
            return {'processed_count': processed_count}
            
        except Exception as e:
            export_metrics_to_prometheus(
                task_name='transform',
                duration=(datetime.now() - start_time).total_seconds(),
                record_count=0,
                status='failure'
            )
            raise

    @task()
    def data_quality_check(transform_result: dict, extract_result: dict, **kwargs):
        """Data Quality: Validate pipeline output"""
        start_time = datetime.now()
        
        try:
            print("🔍 Running data quality checks...")
            
            extracted = extract_result['record_count']
            processed = transform_result['processed_count']
            
            retention_rate = (processed / extracted * 100) if extracted > 0 else 0
            
            checks = {
                'extraction_successful': extracted > 0,
                'transformation_successful': processed > 0,
                'high_retention': retention_rate >= 80,
                'reasonable_volume': 50 <= processed <= 10000,
            }
            
            passed_checks = sum(checks.values())
            quality_score = (passed_checks / len(checks)) * 100
            
            print(f"\n📊 Data Quality Report:")
            print(f"   Extracted: {extracted} records")
            print(f"   Processed: {processed} records")
            print(f"   Retention Rate: {retention_rate:.1f}%")
            print(f"   Quality Score: {quality_score:.1f}%")
            
            export_metrics_to_prometheus(
                task_name='quality_check',
                duration=(datetime.now() - start_time).total_seconds(),
                record_count=processed,
                status='success',
                quality_score=quality_score
            )
            
            if quality_score < 95:
                alert_msg = f"⚠️  Low quality! Score: {quality_score:.1f}%, Retention: {retention_rate:.1f}%"
                send_slack_alert(alert_msg, kwargs)
            
            if retention_rate < 50:
                raise ValueError(f"Critical data loss! {retention_rate:.1f}% retention")
            
            print(f"\n✅ Quality check passed!")
            return {'quality_score': quality_score, 'retention_rate': retention_rate}
            
        except Exception as e:
            export_metrics_to_prometheus(
                task_name='quality_check',
                duration=(datetime.now() - start_time).total_seconds(),
                record_count=0,
                status='failure',
                quality_score=0
            )
            raise

    @task()
    def pipeline_success_notification(**kwargs):
        """Send success notification"""
        context = get_current_context()
        ti = context['ti']
        
        extract_result = ti.xcom_pull(task_ids='extract_clickstream_data')
        quality_result = ti.xcom_pull(task_ids='data_quality_check')
        
        message = (
            f"✅ *Pipeline Completed!*\n"
            f"*Records:* {extract_result['record_count']}\n"
            f"*Quality:* {quality_result['quality_score']:.1f}%"
        )
        
        send_slack_alert(message, kwargs)
        print(message)

    # Task dependencies
    extract_result = extract_clickstream_data()
    load_result = load_to_postgres(extract_result)
    transform_result = transform_with_spark(load_result)
    quality_result = data_quality_check(transform_result, extract_result)
    success_notification = pipeline_success_notification()
    
    extract_result >> load_result >> transform_result >> quality_result >> success_notification