import boto3
import json
import time
import logging
from datetime import datetime
import os
import pytest
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ensure_bucket_exists(s3_client, bucket_name: str) -> None:
    """Create S3 bucket if it doesn't exist"""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} already exists")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == '404':
            logger.info(f"Creating bucket {bucket_name}...")
            try:
                region = s3_client.meta.region_name
                if region == 'us-east-1':
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                logger.info(f"Bucket {bucket_name} created successfully")
            except ClientError as e:
                logger.error(f"Failed to create bucket: {str(e)}")
                raise
        else:
            logger.error(f"Error checking bucket: {str(e)}")
            raise

def generate_test_data():
    """Generate 10 test records"""
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

def run_etl_test(s3_client, glue_client, test_data):
    """Run the ETL job test"""
    bucket_name = "my-spark-processing-bucket"
    test_key = "test/sample_records.json"
    
    try:
        # Ensure bucket exists
        ensure_bucket_exists(s3_client, bucket_name)
        
        # Upload test data to S3
        logger.info("Uploading test data to S3...")
        test_data_json = "\n".join(json.dumps(record) for record in test_data)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=test_data_json.encode('utf-8')
        )
        
        # Start Glue job
        logger.info("Starting Glue job...")
        job_name = "spark-processing-job"
        job_run_id = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                '--source_bucket': bucket_name,
                '--source_key': test_key,
                '--api_endpoint': 'https://jsonplaceholder.typicode.com/posts',  # Test API endpoint
                '--batch_size': '2',
                '--max_workers': '5',
                '--metrics_namespace': 'SparkProcessing/Test'
            }
        )['JobRunId']
        
        # Wait for job completion
        logger.info(f"Waiting for job completion (Run ID: {job_run_id})...")
        while True:
            job_run = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
            status = job_run['JobRun']['JobRunState']
            
            if status in ['SUCCEEDED', 'FAILED', 'TIMEOUT']:
                break
                
            logger.info(f"Job status: {status}")
            time.sleep(30)  # Check every 30 seconds
            
        # Check final status
        if status != 'SUCCEEDED':
            error_message = job_run['JobRun'].get('ErrorMessage', 'No error message available')
            raise Exception(f"Job failed with status: {status}. Error: {error_message}")
        logger.info("Job completed successfully!")
        
        # Log job metrics
        metrics = job_run['JobRun'].get('ExecutionTime', 0)
        logger.info(f"Job execution time: {metrics} seconds")
        
    finally:
        # Cleanup: Delete test data
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=test_key)
            logger.info("Cleaned up test data from S3")
        except Exception as e:
            logger.warning(f"Failed to cleanup test data: {str(e)}")

class TestETLJob:
    @pytest.fixture(scope="class")
    def s3_client(self):
        return boto3.client('s3')
        
    @pytest.fixture(scope="class")
    def glue_client(self):
        return boto3.client('glue')
        
    @pytest.fixture(scope="class")
    def test_data(self):
        return generate_test_data()
        
    def test_etl_job(self, s3_client, glue_client, test_data):
        """Test the ETL job with sample data"""
        run_etl_test(s3_client, glue_client, test_data)

if __name__ == "__main__":
    # Run the test directly
    import sys
    logging.basicConfig(level=logging.INFO)
    
    try:
        s3 = boto3.client('s3')
        glue = boto3.client('glue')
        test_data = generate_test_data()
        
        run_etl_test(s3, glue, test_data)
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        sys.exit(1) 