import pytest
import time
import psutil
import logging
from datetime import datetime
from typing import Dict, List
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from src.processor import SparkProcessor, Config
from src.metrics_collector import MetricsCollector

logger = logging.getLogger(__name__)

class TestLargeScalePerformance:
    @pytest.fixture(scope="class")
    def metrics_collector(self):
        return MetricsCollector()
    
    @pytest.fixture(scope="class")
    def config(self):
        return Config(
            s3_bucket="test-bucket",
            s3_key="test-key",
            api_endpoint="http://test-api.com",
            batch_size=1000,
            max_workers=20,
            rate_limit_calls=5000,
            rate_limit_period=1
        )
    
    @pytest.fixture(scope="class")
    def spark_processor(self, config):
        return SparkProcessor(config)

    def generate_test_data(self, num_records: int) -> pd.DataFrame:
        """Generate test data with specified number of records."""
        np.random.seed(42)
        current_time = datetime.now().isoformat()
        return pd.DataFrame({
            'id': range(num_records),
            'value': np.random.rand(num_records),
            'category': np.random.choice(['A', 'B', 'C'], num_records),
            'timestamp': [current_time for _ in range(num_records)]
        })

    def collect_system_metrics(self) -> Dict:
        """Collect system metrics including CPU and memory usage."""
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        return {
            'cpu_utilization': cpu_percent,
            'memory_total': memory.total,
            'memory_used': memory.used,
            'memory_percent': memory.percent
        }

    def test_process_200m_records(self, spark_processor, metrics_collector):
        """
        Test processing performance with 200 million records.
        Measures:
        - Total processing time
        - Memory utilization
        - CPU utilization
        - Processing rate
        - Worker node metrics
        """
        # Test parameters
        num_records = 200_000_000
        batch_size = 1_000_000
        metrics = []
        
        logger.info(f"Starting performance test with {num_records:,} records")
        
        # Generate test data in batches to avoid memory issues
        total_start_time = time.time()
        processed_records = 0
        
        try:
            for batch_start in range(0, num_records, batch_size):
                batch_end = min(batch_start + batch_size, num_records)
                
                # Generate and process batch
                batch_data = self.generate_test_data(batch_size)
                batch_start_time = time.time()
                
                # Process batch
                spark_processor.process_batch(batch_data)
                
                # Collect metrics
                batch_end_time = time.time()
                batch_duration = batch_end_time - batch_start_time
                system_metrics = self.collect_system_metrics()
                
                processed_records += len(batch_data)
                processing_rate = len(batch_data) / batch_duration
                
                # Worker node metrics
                worker_metrics = spark_processor.get_worker_metrics()
                
                metrics.append({
                    'batch_number': batch_start // batch_size + 1,
                    'records_processed': len(batch_data),
                    'batch_duration': batch_duration,
                    'processing_rate': processing_rate,
                    **system_metrics,
                    'worker_metrics': worker_metrics
                })
                
                # Log progress
                progress = (processed_records / num_records) * 100
                logger.info(f"Progress: {progress:.2f}% ({processed_records:,}/{num_records:,} records)")
                logger.info(f"Current processing rate: {processing_rate:.2f} records/second")
                logger.info(f"CPU Utilization: {system_metrics['cpu_utilization']}%")
                logger.info(f"Memory Utilization: {system_metrics['memory_percent']}%")
                
        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            raise
        
        finally:
            total_duration = time.time() - total_start_time
            
            # Calculate and log final metrics
            avg_processing_rate = processed_records / total_duration
            avg_cpu_utilization = np.mean([m['cpu_utilization'] for m in metrics])
            avg_memory_utilization = np.mean([m['memory_percent'] for m in metrics])
            
            summary = {
                'total_records': num_records,
                'total_duration_seconds': total_duration,
                'avg_processing_rate': avg_processing_rate,
                'avg_cpu_utilization': avg_cpu_utilization,
                'avg_memory_utilization': avg_memory_utilization,
                'peak_memory_usage': max(m['memory_used'] for m in metrics),
                'peak_cpu_usage': max(m['cpu_utilization'] for m in metrics)
            }
            
            logger.info("Performance Test Summary:")
            logger.info(f"Total Duration: {total_duration:.2f} seconds")
            logger.info(f"Average Processing Rate: {avg_processing_rate:.2f} records/second")
            logger.info(f"Average CPU Utilization: {avg_cpu_utilization:.2f}%")
            logger.info(f"Average Memory Utilization: {avg_memory_utilization:.2f}%")
            logger.info(f"Peak Memory Usage: {summary['peak_memory_usage'] / (1024**3):.2f} GB")
            logger.info(f"Peak CPU Usage: {summary['peak_cpu_usage']:.2f}%")
            
            # Save metrics to file
            metrics_df = pd.DataFrame(metrics)
            metrics_df.to_csv('logs/performance_metrics_200m.csv', index=False)
            
            # Assert performance requirements
            assert total_duration < 7200, f"Processing took too long: {total_duration:.2f} seconds"
            assert avg_processing_rate > 10000, f"Processing rate too low: {avg_processing_rate:.2f} records/second"
            assert avg_memory_utilization < 90, f"Memory utilization too high: {avg_memory_utilization:.2f}%"
            
            return summary 

    def test_large_scale_processing(self, spark_processor, metrics_collector):
        """Test processing performance with 1 million records."""
        
        # Test parameters
        num_records = 1_000_000
        batch_size = 100_000  # Reduced batch size for better memory management
        metrics = []
        
        logger.info(f"Starting performance test with {num_records:,} records")
        
        # Generate test data in batches to avoid memory issues
        total_start_time = time.time()
        processed_records = 0
        
        try:
            for batch_start in range(0, num_records, batch_size):
                batch_end = min(batch_start + batch_size, num_records)
                
                # Generate and process batch
                batch_data = self.generate_test_data(batch_size)
                batch_start_time = time.time()
                
                # Process batch
                spark_processor.process_batch(batch_data)
                
                # Collect metrics
                batch_end_time = time.time()
                batch_duration = batch_end_time - batch_start_time
                system_metrics = self.collect_system_metrics()
                
                processed_records += len(batch_data)
                processing_rate = len(batch_data) / batch_duration
                
                # Worker node metrics
                worker_metrics = spark_processor.get_worker_metrics()
                
                metrics.append({
                    'batch_number': batch_start // batch_size + 1,
                    'records_processed': len(batch_data),
                    'batch_duration': batch_duration,
                    'processing_rate': processing_rate,
                    **system_metrics,
                    'worker_metrics': worker_metrics
                })
                
                # Log progress
                progress = (processed_records / num_records) * 100
                logger.info(f"Progress: {progress:.2f}% ({processed_records:,}/{num_records:,} records)")
                logger.info(f"Current processing rate: {processing_rate:.2f} records/second")
                logger.info(f"CPU Utilization: {system_metrics['cpu_utilization']}%")
                logger.info(f"Memory Utilization: {system_metrics['memory_percent']}%")
                
        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            raise
        
        finally:
            total_duration = time.time() - total_start_time
            
            if metrics:  # Only calculate metrics if we have data
                # Calculate and log final metrics
                avg_processing_rate = processed_records / total_duration
                avg_cpu_utilization = np.mean([m['cpu_utilization'] for m in metrics])
                avg_memory_utilization = np.mean([m['memory_percent'] for m in metrics])
                
                summary = {
                    'total_records': num_records,
                    'total_duration_seconds': total_duration,
                    'avg_processing_rate': avg_processing_rate,
                    'avg_cpu_utilization': avg_cpu_utilization,
                    'avg_memory_utilization': avg_memory_utilization,
                    'peak_memory_usage': max(m['memory_used'] for m in metrics),
                    'peak_cpu_usage': max(m['cpu_utilization'] for m in metrics)
                }
                
                logger.info("Performance Test Summary:")
                logger.info(f"Total Duration: {total_duration:.2f} seconds")
                logger.info(f"Average Processing Rate: {avg_processing_rate:.2f} records/second")
                logger.info(f"Average CPU Utilization: {avg_cpu_utilization:.2f}%")
                logger.info(f"Average Memory Utilization: {avg_memory_utilization:.2f}%")
                logger.info(f"Peak Memory Usage: {summary['peak_memory_usage'] / (1024**3):.2f} GB")
                logger.info(f"Peak CPU Usage: {summary['peak_cpu_usage']:.2f}%")
                
                # Save metrics to file
                metrics_df = pd.DataFrame(metrics)
                metrics_df.to_csv('logs/performance_metrics_1m.csv', index=False)
                
                # Assert performance requirements
                assert total_duration < 7200, f"Processing took too long: {total_duration:.2f} seconds"
                assert avg_processing_rate > 10000, f"Processing rate too low: {avg_processing_rate:.2f} records/second"
                assert avg_memory_utilization < 90, f"Memory utilization too high: {avg_memory_utilization:.2f}%"
                
                return summary
            else:
                logger.error("No metrics were collected during the test") 