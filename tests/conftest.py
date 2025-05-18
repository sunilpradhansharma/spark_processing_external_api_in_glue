import pytest
from unittest.mock import Mock
import os
import json
from src.processor import Config
from src.metrics import ProcessingMetrics

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