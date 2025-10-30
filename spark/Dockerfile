FROM apache/spark:3.5.0

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    psycopg2-binary==2.9.9

# Create directories
RUN mkdir -p /opt/spark/jobs /opt/spark/data /opt/spark/jars

# Download PostgreSQL JDBC driver
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
    -o /opt/spark/jars/postgresql-42.6.0.jar

WORKDIR /opt/spark

USER 1001