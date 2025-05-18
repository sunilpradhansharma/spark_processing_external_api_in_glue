import pytest
from src.exceptions import *

def test_api_error():
    error = APIError("Test error", status_code=429, response="Rate limit exceeded")
    assert str(error) == "Test error"
    assert error.status_code == 429
    assert error.response == "Rate limit exceeded"

def test_rate_limit_exceeded():
    error = RateLimitExceeded("Rate limit hit")
    assert isinstance(error, APIError)
    assert str(error) == "Rate limit hit"

def test_s3_error():
    error = S3Error("Failed to access S3", bucket="test-bucket", key="test/file.json")
    assert str(error) == "Failed to access S3"
    assert error.bucket == "test-bucket"
    assert error.key == "test/file.json"

def test_validation_error():
    record = {"id": 1, "data": "invalid"}
    error = ValidationError("Invalid data format", record=record)
    assert str(error) == "Invalid data format"
    assert error.record == record

def test_retryable_error():
    error = RetryableError("Temporary failure", retry_after=60)
    assert str(error) == "Temporary failure"
    assert error.retry_after == 60

def test_resource_exhausted():
    error = ResourceExhaustedError("Memory limit reached", resource_type="memory")
    assert str(error) == "Memory limit reached"
    assert error.resource_type == "memory"

def test_error_hierarchy():
    # Test that all custom errors inherit from ProcessorError
    assert issubclass(APIError, ProcessorError)
    assert issubclass(S3Error, ProcessorError)
    assert issubclass(ConfigurationError, ProcessorError)
    assert issubclass(ValidationError, ProcessorError)
    assert issubclass(RetryableError, ProcessorError)
    assert issubclass(NonRetryableError, ProcessorError)
    
    # Test specific error hierarchies
    assert issubclass(RateLimitExceeded, APIError)
    assert issubclass(APIConnectionError, APIError)
    assert issubclass(APITimeoutError, APIError)
    assert issubclass(S3ReadError, S3Error)
    assert issubclass(S3WriteError, S3Error) 