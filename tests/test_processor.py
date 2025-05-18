import pytest
from unittest.mock import Mock, patch
import os
from src.processor import Config, MemoryEfficientQueue, S3StreamReader, APIClient, Processor

@pytest.fixture
def mock_config():
    return Config(
        s3_bucket="test-bucket",
        s3_key="test/data.csv",
        api_endpoint="https://api.test.com/process"
    )

@pytest.fixture
def mock_queue():
    return MemoryEfficientQueue(maxsize=10)

class TestMemoryEfficientQueue:
    def test_put_get(self):
        queue = MemoryEfficientQueue(maxsize=2)
        queue.put("item1")
        queue.put("item2")
        assert queue.get() == "item1"
        assert queue.get() == "item2"

    def test_processed_count(self):
        queue = MemoryEfficientQueue(maxsize=2)
        queue.increment_processed()
        queue.increment_processed()
        assert queue.processed_count == 2

class TestS3StreamReader:
    @patch('smart_open.open')
    def test_count_records(self, mock_open):
        mock_file = Mock()
        mock_file.__enter__.return_value = ["line1\n", "line2\n", "line3\n"]
        mock_open.return_value = mock_file

        reader = S3StreamReader("test-bucket", "test.csv", 8192)
        assert reader.total_records == 3

    @patch('smart_open.open')
    def test_read_records(self, mock_open):
        mock_file = Mock()
        mock_file.__enter__.return_value = ["line1\n", "line2\n"]
        mock_open.return_value = mock_file

        reader = S3StreamReader("test-bucket", "test.csv", 8192)
        records = list(reader.read_records())
        assert records == ["line1", "line2"]

class TestAPIClient:
    @patch('requests.Session')
    def test_process_record(self, mock_session):
        mock_response = Mock()
        mock_response.json.return_value = {"status": "success"}
        mock_session.return_value.post.return_value = mock_response

        client = APIClient("https://api.test.com", 5000, 1)
        result = client.process_record("test_data")
        assert result == {"status": "success"}

    @patch('requests.Session')
    def test_process_record_error(self, mock_session):
        mock_session.return_value.post.side_effect = Exception("API Error")

        client = APIClient("https://api.test.com", 5000, 1)
        with pytest.raises(Exception):
            client.process_record("test_data")

class TestProcessor:
    @patch('src.processor.S3StreamReader')
    @patch('src.processor.APIClient')
    def test_process_batch(self, mock_api_client, mock_reader, mock_config):
        processor = Processor(mock_config)
        processor.process_batch(["record1", "record2"])
        
        assert mock_api_client.return_value.process_record.call_count == 2
        assert processor.queue.processed_count == 2

    @patch('src.processor.S3StreamReader')
    @patch('src.processor.APIClient')
    def test_process_batch_with_error(self, mock_api_client, mock_reader, mock_config):
        mock_api_client.return_value.process_record.side_effect = Exception("API Error")
        
        processor = Processor(mock_config)
        processor.process_batch(["record1"])
        
        assert processor.error_count == 1 