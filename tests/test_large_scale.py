import pytest
from unittest.mock import Mock, patch, MagicMock
import threading
import queue
import time
from src.processor import Config, Processor, S3StreamReader, APIClient
from concurrent.futures import ThreadPoolExecutor

class MockS3Generator:
    """Mock class to generate large number of records"""
    def __init__(self, num_records: int):
        self.num_records = num_records
        self.current = 0
        
    def __iter__(self):
        return self
        
    def __next__(self):
        if self.current < self.num_records:
            self.current += 1
            return f'{{"id": {self.current}, "data": "mock_data_{self.current}"}}\n'
        raise StopIteration

class MockAPIEndpoint:
    """Mock API endpoint with rate limiting"""
    def __init__(self, rate_limit: int):
        self.rate_limit = rate_limit
        self.current_second = int(time.time())
        self.calls_this_second = 0
        self.lock = threading.Lock()
        
    def process_request(self, data: str) -> dict:
        with self.lock:
            current_time = int(time.time())
            if current_time > self.current_second:
                self.current_second = current_time
                self.calls_this_second = 0
                
            if self.calls_this_second >= self.rate_limit:
                raise Exception("Rate limit exceeded")
                
            self.calls_this_second += 1
            return {"status": "success", "processed": data}

@pytest.fixture
def mock_large_config():
    return Config(
        s3_bucket="mock-bucket",
        s3_key="mock/data.json",
        api_endpoint="https://mock-api.test.com/process",
        batch_size=1000,
        max_workers=20
    )

@pytest.fixture
def mock_api_endpoint():
    return MockAPIEndpoint(rate_limit=5000)

class TestLargeScaleProcessing:
    @patch('smart_open.open')
    @patch('requests.Session')
    def test_process_200m_records(self, mock_session, mock_open, mock_large_config, mock_api_endpoint):
        # Configure mock S3 reader
        total_records = 200_000_000
        mock_file = MagicMock()
        mock_file.__enter__.return_value = MockS3Generator(total_records)
        mock_open.return_value = mock_file
        
        # Configure mock API client
        mock_response = Mock()
        mock_response.json.return_value = {"status": "success"}
        mock_response.raise_for_status.return_value = None
        mock_session.return_value.post.return_value = mock_response
        
        # Create and run processor
        processor = Processor(mock_large_config)
        
        # Start processing
        start_time = time.time()
        processor.run()
        end_time = time.time()
        
        # Assertions
        duration = end_time - start_time
        records_per_second = total_records / duration
        
        assert processor.queue.processed_count == total_records
        assert processor.error_count == 0
        assert records_per_second <= 5000  # Verify rate limit
        
    def test_rate_limit_compliance(self, mock_large_config, mock_api_endpoint):
        """Test that rate limiting is working correctly"""
        api_client = APIClient(
            mock_large_config.api_endpoint,
            mock_large_config.rate_limit_calls,
            mock_large_config.rate_limit_period
        )
        
        # Try to make more calls than the rate limit allows
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for _ in range(6000):  # Try 6000 calls (above 5000 limit)
                futures.append(
                    executor.submit(api_client.process_record, "test_data")
                )
            
            # Count successful and failed calls
            success_count = 0
            error_count = 0
            for future in futures:
                try:
                    future.result()
                    success_count += 1
                except Exception:
                    error_count += 1
                    
            assert success_count <= 5000  # Should not exceed rate limit
            assert error_count > 0  # Should have some failed calls

def test_memory_usage(mock_large_config):
    """Test memory usage during processing"""
    import psutil
    import os
    
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss
    
    # Create processor and run with a smaller sample
    with patch('smart_open.open') as mock_open:
        mock_file = MagicMock()
        mock_file.__enter__.return_value = MockS3Generator(1_000_000)  # 1M records for memory test
        mock_open.return_value = mock_file
        
        processor = Processor(mock_large_config)
        processor.run()
    
    final_memory = process.memory_info().rss
    memory_increase = (final_memory - initial_memory) / 1024 / 1024  # MB
    
    # Memory increase should be reasonable (less than 500MB for 1M records)
    assert memory_increase < 500, f"Memory increase was {memory_increase:.2f}MB" 