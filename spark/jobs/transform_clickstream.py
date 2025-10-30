"""
Clickstream ETL Pipeline with Comprehensive Monitoring

This DAG demonstrates:
- ETL pipeline orchestration
- Self-healing (automatic retries with exponential backoff)
- Prometheus metrics export
- Data quality validation
- Slack alerting
- SLA monitoring
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
import json
import subprocess
import requests
import os
import traceback

# Import our custom utilities
from utils.data_generator import generate_clickstream_data, validate_event_data
from utils.metrics import export_metrics_to_prometheus, export_pipeline_metrics


# Configuration
DAG_ID = 'clickstream_etl_pipeline'
SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK_URL', '')

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=30),
    'sla': timedelta(minutes=30),
}


def send_slack_alert(message, context=None, emoji=":rotating_light:"):
    """
    Send alert to Slack webhook
    
    Args:
        message (str): Alert message
        context (dict, optional): Airflow context
        emoji (str): Slack emoji for message
    """
    if not SLACK_WEBHOOK:
        print("ℹ️  Slack webhook not configured - skipping alert")
        print(f"   Message would be: {message}")
        return
    
    # Build Slack payload
    payload = {
        "text": f"{emoji} *Pipeline Alert*\n{message}",
        "username": "Pipeline Monitor",
        "icon_emoji": emoji
    }
    
    # Add task info if context provided
    if context:
        try:
            task_instance = context.get('task_instance')
            dag = context.get('dag')
            
            if task_instance and dag:
                task_info = (
                    f"\n*Task:* `{task_instance.task_id}`"
                    f"\n*DAG:* `{dag.dag_id}`"
                    f"\n*Execution Date:* {context.get('execution_date')}"
                )
                payload["text"] += task_info
        except Exception as e:
            print(f"⚠️  Error adding context to Slack message: {e}")
    
    # Send to Slack
    try:
        response = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
        response.raise_for_status()
        print(f"✅ Slack alert sent successfully")
    except Exception as e:
        print(f"⚠️  Failed to send Slack alert: {e}")


# Define the DAG
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='End-to-end ETL pipeline with monitoring and self-healing',
    schedule_interval='@hourly',  # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't backfill
    tags=['etl', 'clickstream', 'monitoring', 'production'],
    on_failure_callback=lambda ctx: send_slack_alert("❌ Pipeline FAILED!", ctx, ":x:"),
) as dag:

    @task()
    def extract_clickstream_data(**kwargs):
        """
        Extract Task: Generate clickstream data
        
        Simulates data extraction from various sources.
        In production, this would connect to APIs, databases, or data lakes.
        """
        start_time = datetime.now()
        task_name = 'extract'
        
        try:
            print("=" * 70)
            print("📥 EXTRACT: Generating Clickstream Data")
            print("=" * 70)
            
            # Generate data
            num_events = 1000
            print(f"\n🔄 Generating {num_events} clickstream events...")
            data = generate_clickstream_data(num_events=num_events)
            
            # Validate data - FIX: Correct indentation here
            is_valid, validation_message = validate_event_data(data)
            if not is_valid:
                raise ValueError(f"Data validation failed: {validation_message}")
            
            print(f"✅ Validation passed: {validation_message}")
            
            # Save to file
            execution_date = kwargs['execution_date'].strftime('%Y%m%d_%H%M%S')
            raw_file = f'/opt/airflow/data/raw/clickstream_{execution_date}.json'
            
            with open(raw_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            record_count = len(data)
            duration = (datetime.now() - start_time).total_seconds()
            
            # Export metrics
            export_metrics_to_prometheus(
                task_name=task_name,
                duration=duration,
                record_count=record_count,
                status='success',
                dag_id=DAG_ID
            )
            
            print(f"\n✅ Extract Complete!")
            print(f"   Records: {record_count}")
            print(f"   Duration: {duration:.2f}s")
            print(f"   File: {raw_file}")
            print("=" * 70)
            
            return {
                'file_path': raw_file,
                'record_count': record_count,
                'execution_date': execution_date
            }
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            export_metrics_to_prometheus(
                task_name=task_name,
                duration=duration,
                record_count=0,
                status='failure',
                dag_id=DAG_ID
            )
            print(f"\n❌ Extract failed: {e}")
            traceback.print_exc()
            raise

    @task()
    def load_to_postgres(extract_result: dict, **kwargs):
        """
        Load Task: Store raw data in PostgreSQL
        
        Loads extracted data into the raw_clickstream table.
        Uses upsert logic to handle duplicates.
        """
        start_time = datetime.now()
        task_name = 'load'
        file_path = extract_result['file_path']
        
        try:
            print("=" * 70)
            print("📤 LOAD: Inserting Data into PostgreSQL")
            print("=" * 70)
            
            # Read data file
            print(f"\n📂 Reading file: {file_path}")
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            print(f"✅ Loaded {len(data)} events from file")
            
            # Get PostgreSQL connection
            print(f"\n🔌 Connecting to PostgreSQL...")
            pg_hook = PostgresHook(postgres_conn_id='postgres_default')
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            
            # Prepare insert query with conflict handling
            insert_query = """
                INSERT INTO raw_clickstream 
                (event_id, user_id, session_id, event_type, page_url, timestamp, duration_seconds)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
            """
            
            # Insert data
            print(f"\n💾 Inserting events into database...")
            loaded_count = 0
            skipped_count = 0
            
            for event in data:
                try:
                    cursor.execute(insert_query, (
                        event['event_id'],
                        event['user_id'],
                        event['session_id'],
                        event['event_type'],
                        event['page_url'],
                        event['timestamp'],
                        event.get('duration_seconds', 0)
                    ))
                    if cursor.rowcount > 0:
                        loaded_count += 1
                    else:
                        skipped_count += 1
                except Exception as e:
                    print(f"⚠️  Failed to insert event {event['event_id']}: {e}")
                    continue
            
            # Commit transaction
            conn.commit()
            cursor.close()
            conn.close()
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Export metrics
            export_metrics_to_prometheus(
                task_name=task_name,
                duration=duration,
                record_count=loaded_count,
                status='success',
                dag_id=DAG_ID
            )
            
            print(f"\n✅ Load Complete!")
            print(f"   Inserted: {loaded_count} records")
            print(f"   Skipped (duplicates): {skipped_count} records")
            print(f"   Duration: {duration:.2f}s")
            print("=" * 70)
            
            return {
                'loaded_count': loaded_count,
                'skipped_count': skipped_count
            }
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            export_metrics_to_prometheus(
                task_name=task_name,
                duration=duration,
                record_count=0,
                status='failure',
                dag_id=DAG_ID
            )
            print(f"\n❌ Load failed: {e}")
            traceback.print_exc()
            raise

    @task()
    def transform_with_spark(load_result: dict, extract_result: dict, **kwargs):
        """
        Transform Task: Process data with PySpark
        
        Runs Spark job to:
        - Deduplicate events
        - Calculate session metrics
        - Aggregate page statistics
        """
        start_time = datetime.now()
        task_name = 'transform'
        
        try:
            print("=" * 70)
            print("⚙️  TRANSFORM: Running Spark Job")
            print("=" * 70)
            
            # Prepare Spark job execution
            spark_job_path = '/opt/airflow/spark_jobs/transform_clickstream.py'
            execution_date = extract_result['execution_date']
            
            print(f"\n🎯 Job Details:")
            print(f"   Script: {spark_job_path}")
            print(f"   Execution Date: {execution_date}")
            print(f"   Input Records: {load_result['loaded_count']}")
            
            # Execute Spark job as subprocess
            print(f"\n🚀 Starting Spark transformation...")
            result = subprocess.run(
                ['python3', spark_job_path, execution_date],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            # Check result
            if result.returncode != 0:
                print(f"\n❌ Spark job failed with return code {result.returncode}")
                print(f"\nSTDERR:\n{result.stderr}")
                print(f"\nSTDOUT:\n{result.stdout}")
                raise Exception(f"Spark job failed: {result.stderr}")
            
            # Parse output
            print(f"\n📊 Spark Job Output:")
            print(result.stdout)
            
            # Extract processed count from output
            processed_count = 0
            for line in result.stdout.split('\n'):
                if 'Processed sessions:' in line or 'Sessions Processed:' in line:
                    try:
                        processed_count = int(line.split(':')[-1].strip())
                    except:
                        pass
            
            # If we couldn't parse, query database
            if processed_count == 0:
                print(f"\n🔍 Querying database for processed count...")
                try:
                    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
                    result_query = pg_hook.get_first(
                        "SELECT COUNT(*) FROM processed_sessions WHERE created_at >= NOW() - INTERVAL '1 hour'"
                    )
                    processed_count = result_query[0] if result_query else 0
                except Exception as e:
                    print(f"⚠️  Could not query processed count: {e}")
                    processed_count = load_result['loaded_count']  # Use loaded count as estimate
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Export metrics
            export_metrics_to_prometheus(
                task_name=task_name,
                duration=duration,
                record_count=processed_count,
                status='success',
                dag_id=DAG_ID
            )
            
            print(f"\n✅ Transform Complete!")
            print(f"   Processed: {processed_count} sessions")
            print(f"   Duration: {duration:.2f}s")
            print("=" * 70)
            
            return {
                'processed_count': processed_count,
                'spark_output': result.stdout
            }
            
        except subprocess.TimeoutExpired:
            duration = (datetime.now() - start_time).total_seconds()
            export_metrics_to_prometheus(
                task_name=task_name,
                duration=duration,
                record_count=0,
                status='failure',
                dag_id=DAG_ID
            )
            print(f"\n❌ Spark job timed out after 5 minutes")
            raise
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            export_metrics_to_prometheus(
                task_name=task_name,
                duration=duration,
                record_count=0,
                status='failure',
                dag_id=DAG_ID
            )
            print(f"\n❌ Transform failed: {e}")
            traceback.print_exc()
            raise

    @task()
    def data_quality_check(transform_result: dict, extract_result: dict, load_result: dict, **kwargs):
        """
        Data Quality Task: Validate pipeline output
        
        Checks:
        - Data retention rate (extracted vs processed)
        - Record volume (within expected range)
        - Data completeness
        - Processing efficiency
        """
        start_time = datetime.now()
        task_name = 'quality_check'
        
        try:
            print("=" * 70)
            print("🔍 DATA QUALITY: Running Validation Checks")
            print("=" * 70)
            
            # Get metrics
            extracted = extract_result['record_count']
            loaded = load_result['loaded_count']
            processed = transform_result['processed_count']
            
            print(f"\n📊 Pipeline Metrics:")
            print(f"   Extracted: {extracted} events")
            print(f"   Loaded: {loaded} events")
            print(f"   Processed: {processed} sessions")
            
            # Calculate retention rate
            retention_rate = (processed / extracted * 100) if extracted > 0 else 0
            load_success_rate = (loaded / extracted * 100) if extracted > 0 else 0
            
            print(f"\n📈 Retention Rates:")
            print(f"   Load Success: {load_success_rate:.1f}%")
            print(f"   Overall Retention: {retention_rate:.1f}%")
            
            # Define quality checks
            checks = {
                'extraction_successful': extracted > 0,
                'load_successful': loaded > 0,
                'transformation_successful': processed > 0,
                'high_load_rate': load_success_rate >= 90,
                'acceptable_retention': retention_rate >= 80,
                'reasonable_volume': 50 <= processed <= 10000,
                'no_major_data_loss': retention_rate >= 50,
            }
            
            # Run checks
            print(f"\n✓ Quality Checks:")
            for check_name, check_result in checks.items():
                status = "✅ PASS" if check_result else "❌ FAIL"
                print(f"   {status}: {check_name}")
            
            # Calculate quality score
            passed_checks = sum(checks.values())
            total_checks = len(checks)
            quality_score = (passed_checks / total_checks) * 100
            
            print(f"\n🎯 Overall Quality Score: {quality_score:.1f}%")
            print(f"   Passed: {passed_checks}/{total_checks} checks")
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Export metrics
            export_metrics_to_prometheus(
                task_name=task_name,
                duration=duration,
                record_count=processed,
                status='success',
                quality_score=quality_score,
                dag_id=DAG_ID
            )
            
            # Send alerts for quality issues
            if quality_score < 95:
                alert_msg = (
                    f"⚠️  *Data Quality Alert*\n"
                    f"Quality Score: {quality_score:.1f}%\n"
                    f"Retention Rate: {retention_rate:.1f}%\n"
                    f"Extracted: {extracted} | Processed: {processed}"
                )
                send_slack_alert(alert_msg, kwargs, ":warning:")
            
            # Fail pipeline if critical data loss
            if retention_rate < 50:
                raise ValueError(
                    f"CRITICAL DATA LOSS! Only {retention_rate:.1f}% retention. "
                    f"Extracted: {extracted}, Processed: {processed}"
                )
            
            # Fail if quality too low
            if quality_score < 70:
                raise ValueError(
                    f"Quality score too low: {quality_score:.1f}%. "
                    f"Pipeline quality standards not met."
                )
            
            print(f"\n✅ Quality Check Complete!")
            print("=" * 70)
            
            return {
                'quality_score': quality_score,
                'retention_rate': retention_rate,
                'load_success_rate': load_success_rate,
                'passed_checks': passed_checks,
                'total_checks': total_checks
            }
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            export_metrics_to_prometheus(
                task_name=task_name,
                duration=duration,
                record_count=0,
                status='failure',
                quality_score=0,
                dag_id=DAG_ID
            )
            print(f"\n❌ Quality check failed: {e}")
            traceback.print_exc()
            raise

    @task()
    def pipeline_success_notification(**kwargs):
        """
        Success Task: Send completion notification
        
        Sends summary of successful pipeline execution.
        """
        try:
            print("=" * 70)
            print("🎉 PIPELINE SUCCESS")
            print("=" * 70)
            
            context = get_current_context()
            ti = context['ti']
            
            # Pull results from previous tasks
            extract_result = ti.xcom_pull(task_ids='extract_clickstream_data')
            load_result = ti.xcom_pull(task_ids='load_to_postgres')
            transform_result = ti.xcom_pull(task_ids='transform_with_spark')
            quality_result = ti.xcom_pull(task_ids='data_quality_check')
            
            # Build summary
            summary = (
                f"\n📊 Pipeline Summary:"
                f"\n   Extracted: {extract_result['record_count']} events"
                f"\n   Loaded: {load_result['loaded_count']} events"
                f"\n   Processed: {transform_result['processed_count']} sessions"
                f"\n   Quality Score: {quality_result['quality_score']:.1f}%"
                f"\n   Retention: {quality_result['retention_rate']:.1f}%"
            )
            
            print(summary)
            
            # Send Slack notification
            message = (
                f"✅ *Pipeline Completed Successfully!*\n"
                f"*Records Extracted:* {extract_result['record_count']}\n"
                f"*Sessions Processed:* {transform_result['processed_count']}\n"
                f"*Quality Score:* {quality_result['quality_score']:.1f}%\n"
                f"*Retention Rate:* {quality_result['retention_rate']:.1f}%"
            )
            
            send_slack_alert(message, kwargs, ":white_check_mark:")
            
            print("\n✅ Notification sent!")
            print("=" * 70)
            
        except Exception as e:
            print(f"⚠️  Failed to send success notification: {e}")
            traceback.print_exc()

    # ============================================
    # Define Task Dependencies (Pipeline Flow)
    # ============================================
    
    # Execute tasks in order
    extract_result = extract_clickstream_data()
    load_result = load_to_postgres(extract_result)
    transform_result = transform_with_spark(load_result, extract_result)
    quality_result = data_quality_check(transform_result, extract_result, load_result)
    success_notification = pipeline_success_notification()
    
    # Set dependencies
    extract_result >> load_result >> transform_result >> quality_result >> success_notification