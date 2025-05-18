import pytest
import time
from unittest.mock import Mock, patch
from src.metrics import ProcessingMetrics, CloudWatchMetricsPublisher, MetricsLogger

def test_processing_metrics_initialization():
    metrics = ProcessingMetrics()
    assert metrics.total_records == 0
    assert metrics.processed_records == 0
    assert metrics.failed_records == 0
    assert metrics.rate_limit_hits == 0
    assert metrics.retry_count == 0

def test_record_success():
    metrics = ProcessingMetrics()
    metrics.record_success()
    metrics.record_success()
    assert metrics.processed_records == 2

def test_record_error():
    metrics = ProcessingMetrics()
    metrics.record_error("APIError")
    metrics.record_error("APIError")
    metrics.record_error("ValidationError")
    
    assert metrics.failed_records == 3
    assert metrics.errors["APIError"] == 2
    assert metrics.errors["ValidationError"] == 1

def test_record_api_latency():
    metrics = ProcessingMetrics()
    metrics.record_api_latency(100.0)
    metrics.record_api_latency(200.0)
    
    stats = metrics.get_statistics()
    assert stats["api_latency"]["mean"] == 150.0
    assert stats["api_latency"]["min"] == 100.0
    assert stats["api_latency"]["max"] == 200.0

def test_record_batch_size():
    metrics = ProcessingMetrics()
    metrics.record_batch_size(1000)
    metrics.record_batch_size(2000)
    
    stats = metrics.get_statistics()
    assert stats["batch_statistics"]["mean_size"] == 1500.0
    assert stats["batch_statistics"]["total_batches"] == 2

@patch('boto3.client')
def test_cloudwatch_metrics_publisher(mock_boto3_client):
    mock_cloudwatch = Mock()
    mock_boto3_client.return_value = mock_cloudwatch
    
    publisher = CloudWatchMetricsPublisher("TestNamespace")
    publisher.publish_metric("TestMetric", 100.0, "Count")
    
    mock_cloudwatch.put_metric_data.assert_called_once()
    call_args = mock_cloudwatch.put_metric_data.call_args[1]
    assert call_args["Namespace"] == "TestNamespace"
    assert len(call_args["MetricData"]) == 1
    assert call_args["MetricData"][0]["MetricName"] == "TestMetric"
    assert call_args["MetricData"][0]["Value"] == 100.0
    assert call_args["MetricData"][0]["Unit"] == "Count"

@patch('time.sleep', return_value=None)
def test_metrics_logger(mock_sleep):
    metrics = ProcessingMetrics()
    logger = MetricsLogger(metrics, log_interval=1)
    
    # Record some test data
    metrics.total_records = 1000
    metrics.record_success()
    metrics.record_success()
    metrics.record_error("TestError")
    
    # Start logger and let it run briefly
    logger.start()
    time.sleep(0.1)  # Brief pause to allow one log cycle
    logger.stop()
    
    # Verify metrics were logged
    stats = metrics.get_statistics()
    assert stats["total_records"] == 1000
    assert stats["processed_records"] == 2
    assert stats["failed_records"] == 1
    assert "TestError" in stats["error_distribution"]

def test_metrics_thread_safety():
    import threading
    
    metrics = ProcessingMetrics()
    def worker():
        for _ in range(1000):
            metrics.record_success()
            metrics.record_error("TestError")
            metrics.record_api_latency(100.0)
    
    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    stats = metrics.get_statistics()
    assert stats["processed_records"] == 10000
    assert stats["failed_records"] == 10000
    assert stats["error_distribution"]["TestError"] == 10000 