import pytest
from unittest.mock import Mock
import os
import json
from src.processor import Config
from src.metrics import ProcessingMetrics
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
def test_config():
    return Config(
        s3_bucket="test-bucket",
        s3_key="test/data.json",
        api_endpoint="https://api.test.com/process",
        batch_size=100,
        max_workers=5
    )

@pytest.fixture
def mock_s3_data():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_file = os.path.join(current_dir, 'data', 'mock_records.json')
    with open(data_file, 'r') as f:
        return [json.loads(line) for line in f]

@pytest.fixture
def mock_metrics():
    return ProcessingMetrics()

@pytest.fixture
def mock_api_response():
    response = Mock()
    response.json.return_value = {"status": "success"}
    response.raise_for_status.return_value = None
    return response

@pytest.fixture
def mock_s3_client():
    mock = Mock()
    mock.get_object.return_value = {
        'Body': Mock(read=lambda: b'{"id": 1, "data": "test"}\n')
    }
    return mock

@pytest.fixture
def mock_cloudwatch_client():
    return Mock()

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark = (SparkSession.builder
            .appName("SparkProcessingTest")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
            .config("spark.hadoop.fs.s3a.access.key", "test")
            .config("spark.hadoop.fs.s3a.secret.key", "test")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate())
    
    logger.info("Created Spark session for testing")
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def s3_client():
    """Create a boto3 S3 client for LocalStack"""
    return boto3.client(
        's3',
        endpoint_url='http://localstack:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

@pytest.fixture(scope="session")
def mock_s3_bucket(s3_client):
    """Create and populate a test S3 bucket"""
    bucket_name = "test-spark-processing"
    
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        logger.info(f"Created test bucket: {bucket_name}")
        
        # Generate test data
        test_records = [
            {
                "id": i,
                "name": f"Test User {i}",
                "email": f"user{i}@test.com",
                "phone": f"+1555{i:06d}",
                "address": f"{i} Test St, Test City, TC {i:05d}",
                "created_at": "2024-03-14T12:00:00Z"
            }
            for i in range(1, 11)
        ]
        
        # Upload test data
        s3_client.put_object(
            Bucket=bucket_name,
            Key="test_records.json",
            Body="\n".join(json.dumps(record) for record in test_records).encode()
        )
        logger.info("Uploaded test records to S3")
        
        yield bucket_name
        
        # Cleanup
        for obj in s3_client.list_objects(Bucket=bucket_name).get('Contents', []):
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        s3_client.delete_bucket(Bucket=bucket_name)
        logger.info("Cleaned up test bucket")
        
    except Exception as e:
        logger.error(f"Failed to setup mock S3: {str(e)}")
        raise

@pytest.fixture(scope="function")
def config():
    """Create a test configuration"""
    from src.processor import Config
    return Config(
        s3_bucket="test-spark-processing",
        s3_key="test_records.json",
        api_endpoint="https://jsonplaceholder.typicode.com/posts",
        batch_size=2,
        max_workers=2
    ) 