import pytest
import numpy as np
from unittest.mock import Mock, patch
from src.ml_predictor import PerformancePredictor, AnomalyDetector

class TestPerformancePredictor:
    @pytest.fixture
    def sample_data(self):
        return {
            "cpu_usage": np.array([20.0, 30.0, 40.0, 50.0, 60.0]),
            "memory_usage": np.array([40.0, 45.0, 50.0, 55.0, 60.0]),
            "latency": np.array([100.0, 120.0, 140.0, 160.0, 180.0])
        }
    
    def test_model_training(self, sample_data):
        predictor = PerformancePredictor()
        predictor.train(sample_data)
        
        features = np.array([[40.0, 50.0]])  # cpu, memory
        prediction = predictor.predict_latency(features)
        
        assert isinstance(prediction, float)
        assert 100.0 <= prediction <= 200.0
        
    def test_feature_importance(self, sample_data):
        predictor = PerformancePredictor()
        predictor.train(sample_data)
        
        importance = predictor.get_feature_importance()
        assert len(importance) == 2  # cpu and memory features
        assert all(i >= 0 for i in importance)
        
    def test_prediction_confidence(self, sample_data):
        predictor = PerformancePredictor()
        predictor.train(sample_data)
        
        features = np.array([[40.0, 50.0]])
        prediction, confidence = predictor.predict_with_confidence(features)
        
        assert isinstance(prediction, float)
        assert isinstance(confidence, float)
        assert 0.0 <= confidence <= 1.0

class TestAnomalyDetector:
    @pytest.fixture
    def sample_metrics(self):
        return np.array([
            [20.0, 40.0, 100.0],  # cpu, memory, latency
            [30.0, 45.0, 120.0],
            [40.0, 50.0, 140.0],
            [50.0, 55.0, 160.0],
            [60.0, 60.0, 180.0],
            [90.0, 90.0, 300.0]  # anomaly
        ])
    
    def test_anomaly_detection(self, sample_metrics):
        detector = AnomalyDetector()
        detector.train(sample_metrics)
        
        normal_point = np.array([[40.0, 50.0, 140.0]])
        anomaly_point = np.array([[90.0, 90.0, 300.0]])
        
        assert not detector.is_anomaly(normal_point)
        assert detector.is_anomaly(anomaly_point)
        
    def test_anomaly_score(self, sample_metrics):
        detector = AnomalyDetector()
        detector.train(sample_metrics)
        
        point = np.array([[40.0, 50.0, 140.0]])
        score = detector.get_anomaly_score(point)
        
        assert isinstance(score, float)
        assert score >= 0.0
        
    def test_batch_detection(self, sample_metrics):
        detector = AnomalyDetector()
        detector.train(sample_metrics)
        
        batch = np.array([
            [40.0, 50.0, 140.0],  # normal
            [90.0, 90.0, 300.0],  # anomaly
            [45.0, 55.0, 150.0]   # normal
        ])
        
        results = detector.detect_batch(batch)
        assert len(results) == 3
        assert not results[0]  # normal
        assert results[1]      # anomaly
        assert not results[2]  # normal 