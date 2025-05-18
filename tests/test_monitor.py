import pytest
from unittest.mock import Mock, patch
from src.monitor import TelemetryManager, HealthCheck, SystemMonitor

class TestTelemetryManager:
    @patch('opentelemetry.trace.get_tracer_provider')
    def test_trace_context(self, mock_tracer_provider):
        mock_tracer = Mock()
        mock_span = Mock()
        mock_tracer.start_span.return_value = mock_span
        mock_tracer_provider.return_value.get_tracer.return_value = mock_tracer
        
        telemetry = TelemetryManager()
        with telemetry.trace_operation("test_operation"):
            pass
            
        mock_tracer.start_span.assert_called_once_with("test_operation")
        mock_span.end.assert_called_once()
        
    def test_record_metric(self):
        telemetry = TelemetryManager()
        telemetry.record_metric("test_metric", 100.0, {"dimension": "test"})
        
        metrics = telemetry.get_metrics()
        assert "test_metric" in metrics
        assert metrics["test_metric"]["value"] == 100.0
        assert metrics["test_metric"]["dimensions"]["dimension"] == "test"

class TestHealthCheck:
    def test_system_health_check(self):
        health_check = HealthCheck()
        status = health_check.check_system_health()
        
        assert "cpu_usage" in status
        assert "memory_usage" in status
        assert "disk_usage" in status
        
    @patch('psutil.Process')
    def test_process_health_check(self, mock_process):
        mock_process.return_value.memory_info.return_value.rss = 1024 * 1024
        mock_process.return_value.cpu_percent.return_value = 5.0
        
        health_check = HealthCheck()
        status = health_check.check_process_health()
        
        assert status["memory_mb"] == 1.0
        assert status["cpu_percent"] == 5.0

class TestSystemMonitor:
    def test_resource_monitoring(self):
        monitor = SystemMonitor()
        stats = monitor.get_resource_stats()
        
        assert "cpu_percent" in stats
        assert "memory_percent" in stats
        assert "disk_percent" in stats
        assert "network_io" in stats
        
    def test_performance_alerts(self):
        monitor = SystemMonitor()
        monitor.set_threshold("cpu_percent", 90.0)
        monitor.set_threshold("memory_percent", 85.0)
        
        alerts = monitor.check_thresholds({
            "cpu_percent": 95.0,
            "memory_percent": 80.0
        })
        
        assert len(alerts) == 1
        assert alerts[0]["metric"] == "cpu_percent"
        assert alerts[0]["threshold"] == 90.0
        assert alerts[0]["current_value"] == 95.0 