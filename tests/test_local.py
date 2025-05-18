import os
import json
import pytest
import logging
import boto3
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
def s3_client():
    """Create a boto3 S3 client for LocalStack"""
    return boto3.client(
        's3',
        endpoint_url='http://localstack:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

@pytest.fixture(scope="function")
def mock_s3_bucket(s3_client, test_data):
    """Create mock S3 bucket and upload test data"""
    bucket_name = "test-bucket"
    key = "test_records.json"
    
    # Create bucket
    s3_client.create_bucket(Bucket=bucket_name)
    logger.info(f"Created test bucket: {bucket_name}")
    
    # Upload test data
    test_data_str = "\n".join(json.dumps(record) for record in test_data)
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=test_data_str)
    logger.info(f"Uploaded test data to s3://{bucket_name}/{key}")
    
    yield bucket_name
    
    # Cleanup
    try:
        objects = s3_client.list_objects(Bucket=bucket_name).get('Contents', [])
        for obj in objects:
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        s3_client.delete_bucket(Bucket=bucket_name)
        logger.info(f"Cleaned up test bucket: {bucket_name}")
    except Exception as e:
        logger.warning(f"Failed to cleanup test bucket: {str(e)}")

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
    async with APIClient(
        endpoint="https://jsonplaceholder.typicode.com/posts",
        rate_limit=100,
        rate_period=1
    ) as client:
        data = {"title": "Test Post", "body": "Test Content", "userId": 1}
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
    
    # Check metrics
    assert metrics.total_records == 10
    assert metrics.processed_records + sum(metrics.errors.values()) == 10
    assert len(metrics.api_latencies) == metrics.processed_records
    
    # Check results
    result_count = result_df.count()
    assert result_count == 10
    
    # Check that we have either valid responses or error messages
    responses = result_df.select("api_response").collect()
    for row in responses:
        response = json.loads(row.api_response)
        if response["success"]:
            assert "id" in response["data"]
        else:
            assert "error" in response

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
    if metrics.api_latencies:
        logger.info(f"Average API latency: {sum(metrics.api_latencies) / len(metrics.api_latencies):.2f}s")
    if metrics.errors:
        logger.info(f"Error count: {dict(metrics.errors)}")
    
    assert metrics.total_records == 10
    assert metrics.processed_records + sum(metrics.errors.values()) == 10

if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 