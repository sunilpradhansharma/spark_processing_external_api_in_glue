import os
import json
import pytest
import logging
import boto3
from moto import mock_s3
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import aiohttp
import asyncio
from src.processor import Config, APIClient, BatchProcessor
from src.metrics import ProcessingMetrics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    return (SparkSession.builder
            .appName("SparkProcessingTest")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .config("spark.hadoop.fs.s3a.access.key", "test")
            .config("spark.hadoop.fs.s3a.secret.key", "test")
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate())

@pytest.fixture(scope="function")
def test_data():
    """Generate test records"""
    return [
        {
            "id": i,
            "name": f"Test Record {i}",
            "email": f"test{i}@example.com",
            "phone": f"+1-555-000-{i:04d}",
            "address": {
                "street": f"{i} Test Street",
                "city": "Test City",
                "state": "TS",
                "zip": f"1000{i}"
            },
            "created_at": datetime.now().isoformat()
        }
        for i in range(1, 11)
    ]

@pytest.fixture(scope="function")
def mock_s3_bucket(test_data):
    """Create mock S3 bucket and upload test data"""
    with mock_s3():
        s3 = boto3.client('s3')
        bucket_name = "test-bucket"
        key = "test_records.json"
        
        # Create bucket
        s3.create_bucket(Bucket=bucket_name)
        
        # Upload test data
        test_data_str = "\n".join(json.dumps(record) for record in test_data)
        s3.put_object(Bucket=bucket_name, Key=key, Body=test_data_str)
        
        yield bucket_name

@pytest.fixture(scope="function")
def config(mock_s3_bucket):
    """Create test configuration"""
    return Config(
        s3_bucket=mock_s3_bucket,
        s3_key="test_records.json",
        api_endpoint="https://jsonplaceholder.typicode.com/posts",
        batch_size=2,
        max_workers=2,
        rate_limit=100,
        rate_limit_period=1
    )

@pytest.mark.asyncio
async def test_api_client():
    """Test API client functionality"""
    client = APIClient(
        endpoint="https://jsonplaceholder.typicode.com/posts",
        rate_limit=100,
        rate_period=1
    )
    data = {"title": "Test Post", "body": "Test Content", "userId": 1}
    
    async with aiohttp.ClientSession() as session:
        response = await client.process_record(data)
        assert response is not None
        assert "id" in response
        assert response["title"] == data["title"]

def test_batch_processor(spark, mock_s3_bucket, config):
    """Test batch processing functionality"""
    # Read input data
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("created_at", StringType(), True)
    ])
    
    input_path = f"s3a://{mock_s3_bucket}/test_records.json"
    df = spark.read.json(input_path, schema=schema)
    assert df.count() == 10
    
    # Initialize processor
    processor = BatchProcessor(config)
    metrics = ProcessingMetrics()
    
    # Process records
    result_df = processor.process_batch(df, metrics)
    assert result_df is not None
    assert result_df.count() > 0
    
    # Check metrics
    assert metrics.total_records == 10
    assert metrics.processed_records > 0
    assert len(metrics.api_latencies) > 0

def test_end_to_end(spark, mock_s3_bucket, config):
    """Test end-to-end processing"""
    # Read and process data
    input_path = f"s3a://{mock_s3_bucket}/test_records.json"
    df = spark.read.json(input_path)
    processor = BatchProcessor(config)
    metrics = ProcessingMetrics()
    
    # Process records
    result_df = processor.process_batch(df, metrics)
    
    # Write output
    output_path = f"s3a://{mock_s3_bucket}/processed_records"
    result_df.write.mode("overwrite").json(output_path)
    
    # Verify output
    output_df = spark.read.json(output_path)
    assert output_df.count() == df.count()
    
    # Check metrics
    logger.info(f"Total records: {metrics.total_records}")
    logger.info(f"Processed records: {metrics.processed_records}")
    logger.info(f"Average API latency: {sum(metrics.api_latencies) / len(metrics.api_latencies):.2f}s")
    logger.info(f"Error count: {dict(metrics.error_count)}")
    
    assert metrics.total_records == 10
    assert metrics.processed_records == 10
    assert len(metrics.api_latencies) == 10
    assert sum(metrics.error_count.values()) == 0

if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 