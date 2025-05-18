import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from typing import Dict, List, Any, Tuple
import joblib
import os
from datetime import datetime, timedelta

class PerformancePredictor:
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.feature_names = ["cpu_usage", "memory_usage"]
        self.target_name = "latency"
        
    def train(self, data: Dict[str, np.ndarray]) -> None:
        """Train the performance prediction model"""
        X = np.column_stack([data[feature] for feature in self.feature_names])
        y = data[self.target_name]
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Train model (using a simple linear regression for demonstration)
        self.model = np.linalg.lstsq(X_scaled, y, rcond=None)[0]
        
    def predict_latency(self, features: np.ndarray) -> float:
        """Predict latency based on system metrics"""
        if self.model is None:
            raise ValueError("Model not trained")
            
        X_scaled = self.scaler.transform(features.reshape(1, -1))
        return float(X_scaled.dot(self.model))
        
    def save_model(self, path: str) -> None:
        """Save the trained model to disk"""
        if self.model is None:
            raise ValueError("No model to save")
            
        os.makedirs(os.path.dirname(path), exist_ok=True)
        joblib.dump({
            'model': self.model,
            'scaler': self.scaler,
            'feature_names': self.feature_names
        }, path)
        
    def load_model(self, path: str) -> None:
        """Load a trained model from disk"""
        if not os.path.exists(path):
            raise FileNotFoundError(f"Model file not found: {path}")
            
        data = joblib.load(path)
        self.model = data['model']
        self.scaler = data['scaler']
        self.feature_names = data['feature_names']

class AnomalyDetector:
    def __init__(self, contamination: float = 0.1):
        self.model = IsolationForest(contamination=contamination, random_state=42)
        self.scaler = StandardScaler()
        self.history: List[Dict[str, Any]] = []
        self.window_size = timedelta(hours=1)
        
    def fit(self, data: np.ndarray) -> None:
        """Train the anomaly detection model"""
        X_scaled = self.scaler.fit_transform(data)
        self.model.fit(X_scaled)
        
    def predict(self, data: np.ndarray) -> np.ndarray:
        """Predict anomalies in the data"""
        X_scaled = self.scaler.transform(data)
        return self.model.predict(X_scaled)
        
    def record_metrics(self, metrics: Dict[str, float]) -> None:
        """Record metrics for anomaly detection"""
        metrics['timestamp'] = datetime.utcnow()
        self.history.append(metrics)
        
        # Remove old metrics outside the window
        cutoff = datetime.utcnow() - self.window_size
        self.history = [m for m in self.history if m['timestamp'] > cutoff]
        
    def check_anomalies(self) -> List[Dict[str, Any]]:
        """Check for anomalies in recent metrics"""
        if not self.history:
            return []
            
        # Extract features
        features = []
        for metrics in self.history:
            features.append([
                metrics.get('cpu_usage', 0),
                metrics.get('memory_usage', 0),
                metrics.get('latency', 0)
            ])
            
        X = np.array(features)
        predictions = self.predict(X)
        
        # Return anomalies with their timestamps
        anomalies = []
        for i, pred in enumerate(predictions):
            if pred == -1:  # Anomaly
                anomalies.append({
                    'timestamp': self.history[i]['timestamp'],
                    'metrics': self.history[i]
                })
                
        return anomalies
        
    def get_anomaly_stats(self) -> Dict[str, Any]:
        """Get statistics about detected anomalies"""
        anomalies = self.check_anomalies()
        total_points = len(self.history)
        
        return {
            'total_points': total_points,
            'anomaly_count': len(anomalies),
            'anomaly_rate': len(anomalies) / total_points if total_points > 0 else 0,
            'last_anomaly': anomalies[-1] if anomalies else None
        } 