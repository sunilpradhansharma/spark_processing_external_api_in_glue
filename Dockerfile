FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY tests/requirements-local.txt .
RUN pip3 install -r requirements-local.txt

# Download and install AWS Hadoop dependencies
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P $SPARK_HOME/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.261/aws-java-sdk-bundle-1.12.261.jar -P $SPARK_HOME/jars/

# Copy source code
COPY src/ /app/src/
COPY tests/ /app/tests/
COPY glue_script.py /app/

# Set environment variables
ENV PYTHONPATH=/app
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV AWS_ACCESS_KEY_ID=test
ENV AWS_SECRET_ACCESS_KEY=test
ENV AWS_DEFAULT_REGION=us-east-1

# Create directories for test data and logs
RUN mkdir -p /app/data/input /app/data/output /app/logs && \
    chown -R jovyan:users /app/data /app/logs

# Switch back to non-root user
USER jovyan

# Default command
CMD ["python3", "-m", "pytest", "tests/test_local.py", "-v"] 