import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def main(execution_date):
    print(f"🚀 Starting Spark transformation for {execution_date}")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ClickstreamTransform") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()
    
    try:
        # Read from PostgreSQL
        jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
        connection_properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver"
        }
        
        df = spark.read.jdbc(
            url=jdbc_url,
            table="raw_clickstream",
            properties=connection_properties
        )
        
        print(f"📊 Loaded {df.count()} raw events")
        
        # Deduplicate
        df_dedup = df.dropDuplicates(['event_id'])
        
        # Calculate session metrics
        session_stats = df_dedup.groupBy('session_id', 'user_id').agg(
            count('event_id').alias('event_count'),
            sum('duration_seconds').alias('total_duration'),
            min('timestamp').alias('session_start'),
            max('timestamp').alias('session_end'),
            collect_list('page_url').alias('pages_visited')
        )
        
        # Add session duration
        session_stats = session_stats.withColumn(
            'session_duration_minutes',
            (unix_timestamp('session_end') - unix_timestamp('session_start')) / 60
        )
        
        # Page aggregations
        page_stats = df_dedup.groupBy('page_url').agg(
            count('event_id').alias('page_views'),
            countDistinct('user_id').alias('unique_visitors'),
            avg('duration_seconds').alias('avg_time_on_page')
        )
        
        # Write results back to PostgreSQL
        session_stats.write.jdbc(
            url=jdbc_url,
            table="processed_sessions",
            mode="append",
            properties=connection_properties
        )
        
        processed_count = session_stats.count()
        print(f"✅ Processed sessions: {processed_count}")
        print(f"✅ Unique pages: {page_stats.count()}")
        
        # Display sample results
        print("\n📈 Sample Session Stats:")
        session_stats.select(
            'user_id', 'event_count', 'total_duration', 'session_duration_minutes'
        ).show(5, truncate=False)
        
        return 0
        
    except Exception as e:
        print(f"❌ Spark job failed: {e}")
        return 1
    
    finally:
        spark.stop()

if __name__ == "__main__":
    execution_date = sys.argv[1] if len(sys.argv) > 1 else "manual"
    exit_code = main(execution_date)
    sys.exit(exit_code)