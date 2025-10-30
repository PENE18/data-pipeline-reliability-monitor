FROM apache/airflow:2.7.3-python3.11

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.7.1 \
    prometheus-client==0.19.0 \
    psycopg2-binary==2.9.9 \
    requests==2.31.0 \
    kafka-python==2.0.2


#FROM apache/airflow:2.7.3-python3.11

#USER root

# Install system dependencies if needed
#RUN apt-get update && \
 #   apt-get install -y --no-install-recommends \
  #  build-essential \
   # && apt-get clean

#USER airflow

# Install Python packages
#COPY requirements.txt /tmp/
#RUN pip install --no-cache-dir -r /tmp/requirements.txt