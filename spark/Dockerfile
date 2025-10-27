# Spark + Python + Kafka Connectors
FROM apache/spark:3.5.0

USER root

# Install dependencies
RUN apt-get update && apt-get install -y \
    python3 python3-pip openjdk-11-jdk curl vim wget && \
    apt-get clean

# Environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install Python libraries
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
        pyspark==3.5.0 \
        pandas==2.0.3 \
        psycopg2-binary \
        kafka-python

# Add PostgreSQL JDBC driver
RUN mkdir -p /opt/spark/jars && \
    curl -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
    -o /opt/spark/jars/postgresql-42.6.0.jar

# Add Kafka connector jars
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Create directories
RUN mkdir -p /opt/spark/jobs /opt/spark/data

WORKDIR /opt/spark-apps

USER 1001